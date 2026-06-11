from __future__ import annotations

import asyncio
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace


from app.repost_campaign_launch_job_service import RepostCampaignLaunchJobService


class FakeLaunchJobRepo:
    def __init__(self):
        self.jobs = {}
        self.next_id = 1
        self.runs = []
        self.create_calls = []

    def _copy(self, job):
        return deepcopy(job) if job else None

    def create_repost_campaign_launch_job(self, *, rule_id, admin_id, progress_chat_id, progress_message_id, payload_json=None, max_attempts=3):
        self.create_calls.append(
            {
                "rule_id": rule_id,
                "admin_id": admin_id,
                "progress_chat_id": progress_chat_id,
                "progress_message_id": progress_message_id,
                "payload_json": deepcopy(payload_json),
                "max_attempts": max_attempts,
            }
        )
        existing = self.get_active_repost_campaign_launch_job_for_rule(rule_id)
        if existing:
            return existing
        job = {
            "id": self.next_id,
            "rule_id": int(rule_id),
            "admin_id": admin_id,
            "status": "pending",
            "payload_json": payload_json or {},
            "result_json": None,
            "campaign_run_id": None,
            "progress_chat_id": progress_chat_id,
            "progress_message_id": progress_message_id,
            "attempts": 0,
            "max_attempts": max_attempts,
            "locked_by": None,
            "locked_until": None,
            "last_error": None,
        }
        self.jobs[self.next_id] = job
        self.next_id += 1
        return self._copy(job)

    def get_active_repost_campaign_launch_job_for_rule(self, rule_id):
        active = [j for j in self.jobs.values() if j["rule_id"] == int(rule_id) and j["status"] in {"pending", "processing"}]
        return self._copy(active[-1]) if active else None

    def get_repost_campaign_launch_job(self, job_id):
        return self._copy(self.jobs.get(int(job_id)))

    def get_due_repost_campaign_launch_jobs(self, limit=5):
        rows = [j for j in self.jobs.values() if j["status"] == "pending"]
        return [self._copy(j) for j in rows[:limit]]

    def lease_repost_campaign_launch_job(self, job_id, worker_id, lock_ttl_seconds):
        job = self.jobs.get(int(job_id))
        if not job or job["status"] != "pending":
            return None
        job["status"] = "processing"
        job["attempts"] += 1
        job["locked_by"] = worker_id
        job["locked_until"] = "future"
        return self._copy(job)

    def mark_repost_campaign_launch_job_processing(self, job_id, *, worker_id):
        job = self.jobs[int(job_id)]
        job["status"] = "processing"
        job["locked_by"] = worker_id
        return self._copy(job)

    def set_repost_campaign_launch_job_campaign_run_id(self, job_id, campaign_run_id):
        job = self.jobs.get(int(job_id))
        if not job or job["status"] != "processing":
            return None
        job["campaign_run_id"] = int(campaign_run_id)
        return self._copy(job)

    def mark_repost_campaign_launch_job_sent(self, job_id, *, campaign_run_id, result_json):
        job = self.jobs[int(job_id)]
        job.update(status="sent", campaign_run_id=campaign_run_id, result_json=result_json, locked_by=None, locked_until=None)
        return self._copy(job)

    def mark_repost_campaign_launch_job_failed(self, job_id, *, last_error, result_json=None, retryable=False):
        job = self.jobs[int(job_id)]
        job["last_error"] = last_error
        if result_json is not None:
            job["result_json"] = result_json
        job["locked_by"] = None
        job["locked_until"] = None
        job["status"] = "pending" if retryable and job["campaign_run_id"] is None and job["attempts"] < job["max_attempts"] else "failed"
        return self._copy(job)

    def mark_repost_campaign_launch_job_needs_review(self, job_id, *, last_error, campaign_run_id=None, result_json=None):
        job = self.jobs[int(job_id)]
        update = {
            "status": "needs_review",
            "last_error": last_error,
            "campaign_run_id": campaign_run_id or job.get("campaign_run_id"),
            "locked_by": None,
            "locked_until": None,
        }
        if result_json is not None:
            update["result_json"] = result_json
        job.update(update)
        return self._copy(job)

    def mark_repost_campaign_launch_job_cancelled(self, job_id, *, reason):
        job = self.jobs[int(job_id)]
        job.update(status="cancelled", last_error=reason, locked_by=None, locked_until=None)
        return self._copy(job)

    def recover_stale_repost_campaign_launch_jobs(self, *, worker_id=None):
        summary = {"requeued": 0, "needs_review": 0, "failed": 0}
        for job in self.jobs.values():
            if job["status"] != "processing" or job.get("locked_until") != "expired":
                continue
            if job.get("campaign_run_id"):
                job["status"] = "needs_review"
                summary["needs_review"] += 1
            elif job["attempts"] >= job["max_attempts"]:
                job["status"] = "failed"
                summary["failed"] += 1
            else:
                job["status"] = "pending"
                job["locked_by"] = None
                summary["requeued"] += 1
        return summary

    def list_campaign_runs_for_rule(self, rule_id, *, limit=20):
        rows = [r for r in self.runs if r["rule_id"] == int(rule_id)]
        return list(reversed(rows))[:limit]


class FakeRuntime:
    def __init__(
        self,
        *,
        can_launch=True,
        raises=False,
        creates_run_before_raise=False,
        result_ok=True,
        error_text=None,
        campaign_run_id=42,
        extra=None,
        fail_on_readiness=False,
    ):
        self.can_launch = can_launch
        self.raises = raises
        self.creates_run_before_raise = creates_run_before_raise
        self.result_ok = result_ok
        self.error_text = error_text
        self.campaign_run_id = campaign_run_id
        self.extra = extra or {}
        self.fail_on_readiness = fail_on_readiness
        self.launch_calls = 0
        self.readiness_calls = 0
        self.last_launch_kwargs = None
        self.repo = None

    def build_campaign_launch_readiness(self, *, rule_id):
        self.readiness_calls += 1
        if self.fail_on_readiness:
            raise AssertionError("build_campaign_launch_readiness must not be called by launch job service")
        return {"can_launch": self.can_launch, "rule_id": rule_id, "will_send_total": 1}

    async def launch_campaign_now(
        self,
        *,
        rule_id,
        admin_id=None,
        run_type="manual",
        on_campaign_run_created=None,
        force_ignore_clean_channel=False,
    ):
        self.launch_calls += 1
        self.last_launch_kwargs = {
            "rule_id": rule_id,
            "admin_id": admin_id,
            "run_type": run_type,
            "force_ignore_clean_channel": force_ignore_clean_channel,
        }
        if self.creates_run_before_raise:
            self.repo.runs.append({"id": 77, "rule_id": int(rule_id)})
            if on_campaign_run_created:
                on_campaign_run_created(77)
        if self.raises:
            raise RuntimeError("boom")
        extra = {"targets_success": 1, "targets_failed": 0, **self.extra}
        if self.campaign_run_id is not None:
            extra["campaign_run_id"] = int(self.campaign_run_id)
            self.repo.runs.append({"id": int(self.campaign_run_id), "rule_id": int(rule_id)})
            if on_campaign_run_created:
                on_campaign_run_created(int(self.campaign_run_id))
        result = {"ok": self.result_ok, "extra": extra}
        if self.error_text:
            result["error_text"] = self.error_text
        return SimpleNamespace(to_dict=lambda: result)


def make_service(runtime=None):
    repo = FakeLaunchJobRepo()
    runtime = runtime or FakeRuntime()
    runtime.repo = repo
    return repo, runtime, RepostCampaignLaunchJobService(repo=repo, campaign_runtime=runtime)


def test_enqueue_creates_pending_job():
    repo, _, service = make_service()
    result = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=-100, progress_message_id=5)
    assert result.created is True
    assert result.job["status"] == "pending"
    assert result.job["rule_id"] == 4
    assert repo.create_calls[0]["payload_json"]["force_ignore_clean_channel"] is False


def test_enqueue_force_stores_force_true():
    repo, _, service = make_service()
    result = service.enqueue_manual_launch(
        rule_id=4,
        admin_id=10,
        progress_chat_id=-100,
        progress_message_id=5,
        force_ignore_clean_channel=True,
    )
    assert result.created is True
    assert repo.create_calls[0]["payload_json"]["force_ignore_clean_channel"] is True
    assert result.job["payload_json"]["force_ignore_clean_channel"] is True


def test_enqueue_suppresses_duplicate_pending_or_processing_job_for_same_rule():
    repo, _, service = make_service()
    first = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=-100, progress_message_id=5)
    existing_payload = deepcopy(first.job["payload_json"])
    second = service.enqueue_manual_launch(
        rule_id=4,
        admin_id=10,
        progress_chat_id=-100,
        progress_message_id=6,
        force_ignore_clean_channel=True,
    )
    assert second.created is False
    assert second.job["id"] == first.job["id"]
    assert len(repo.create_calls) == 1
    assert second.job["payload_json"] == existing_payload
    assert repo.jobs[first.job["id"]]["payload_json"] == existing_payload


def test_worker_leases_pending_job():
    async def _run():
        repo, _, service = make_service()
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        await service.process_due_jobs(worker_id="w1", limit=1)
        assert repo.jobs[job["id"]]["locked_by"] is None
        assert repo.jobs[job["id"]]["status"] == "sent"
    asyncio.run(_run())


def test_successful_job_calls_runtime_once_and_saves_result_and_run_id():
    async def _run():
        repo, runtime, service = make_service()
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert runtime.last_launch_kwargs["force_ignore_clean_channel"] is False
        assert stored["status"] == "sent"
        assert stored["campaign_run_id"] == 42
        assert stored["result_json"]["extra"]["targets_success"] == 1
    asyncio.run(_run())



def test_launch_job_marks_needs_review_when_result_not_ok_after_campaign_run_created():
    async def _run():
        repo, runtime, service = make_service(
            FakeRuntime(
                result_ok=False,
                error_text="Не удалось подтвердить отправку",
                campaign_run_id=123,
            )
        )
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert stored["status"] == "needs_review"
        assert stored["status"] != "sent"
        assert stored["campaign_run_id"] == 123
        assert stored["last_error"] == "Не удалось подтвердить отправку"
        assert stored["result_json"]["extra"]["campaign_run_id"] == 123
    asyncio.run(_run())


def test_launch_job_marks_failed_when_result_not_ok_without_campaign_run():
    async def _run():
        repo, runtime, service = make_service(
            FakeRuntime(
                result_ok=False,
                error_text="Запуск заблокирован",
                campaign_run_id=None,
            )
        )
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert stored["status"] == "failed"
        assert stored["status"] != "sent"
        assert stored["campaign_run_id"] is None
        assert stored["last_error"] == "Запуск заблокирован"
        assert stored["result_json"] == {"ok": False, "extra": {"targets_success": 1, "targets_failed": 0}, "error_text": "Запуск заблокирован"}
    asyncio.run(_run())


def test_run_once_passes_force_true_from_payload():
    async def _run():
        repo, runtime, service = make_service()
        job = service.enqueue_manual_launch(
            rule_id=4,
            admin_id=10,
            progress_chat_id=None,
            progress_message_id=None,
            force_ignore_clean_channel=True,
        ).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert runtime.last_launch_kwargs["force_ignore_clean_channel"] is True
        assert stored["status"] == "sent"
    asyncio.run(_run())


def test_run_once_confirm_required_becomes_failed_job():
    async def _run():
        repo, runtime, service = make_service(
            FakeRuntime(
                result_ok=False,
                error_text="Нужно подтвердить запуск поверх активной рекламы",
                campaign_run_id=None,
                extra={
                    "requires_clean_channel_confirmation": True,
                    "manual_launch_policy": {"action": "confirm_required"},
                },
            )
        )
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert stored["status"] == "failed"
        assert stored["last_error"] == "Нужно подтвердить запуск поверх активной рекламы"
        assert stored["campaign_run_id"] is None
        assert "campaign_run_id" not in stored["result_json"]["extra"]
        assert stored["result_json"]["extra"]["requires_clean_channel_confirmation"] is True
        assert stored["result_json"]["extra"]["manual_launch_policy"] == {"action": "confirm_required"}
    asyncio.run(_run())


def test_run_once_clean_channel_blocked_becomes_failed_job():
    async def _run():
        repo, runtime, service = make_service(
            FakeRuntime(
                result_ok=False,
                error_text="Чистый канал не позволяет запустить кампанию поверх активной рекламы",
                campaign_run_id=None,
                extra={
                    "clean_channel_blocked": True,
                    "manual_launch_policy": {"action": "block"},
                },
            )
        )
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert stored["status"] == "failed"
        assert stored["last_error"] == "Чистый канал не позволяет запустить кампанию поверх активной рекламы"
        assert stored["campaign_run_id"] is None
        assert stored["result_json"]["extra"]["clean_channel_blocked"] is True
        assert stored["result_json"]["extra"]["manual_launch_policy"] == {"action": "block"}
    asyncio.run(_run())


def test_run_once_no_longer_calls_readiness_before_runtime_launch():
    async def _run():
        repo, runtime, service = make_service(FakeRuntime(fail_on_readiness=True))
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.readiness_calls == 0
        assert runtime.launch_calls == 1
        assert stored["status"] == "sent"
    asyncio.run(_run())


def test_exception_before_campaign_run_increments_attempts_and_can_retry():
    async def _run():
        repo, _, service = make_service(FakeRuntime(raises=True))
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert stored["attempts"] == 1
        assert stored["status"] == "pending"
    asyncio.run(_run())


def test_exception_after_campaign_run_becomes_needs_review():
    async def _run():
        repo, runtime, service = make_service(FakeRuntime(raises=True, creates_run_before_raise=True))
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert stored["status"] == "needs_review"
        assert stored["campaign_run_id"] == 77
    asyncio.run(_run())


def test_stale_processing_without_campaign_run_recovers_to_pending():
    async def _run():
        repo, _, service = make_service()
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        repo.jobs[job["id"]].update(status="processing", locked_until="expired", attempts=1)
        summary = await service.recover_stale_jobs(worker_id="w1")
        assert summary["requeued"] == 1
        assert repo.jobs[job["id"]]["status"] == "pending"
    asyncio.run(_run())


def test_stale_processing_with_campaign_run_moves_to_needs_review():
    async def _run():
        repo, _, service = make_service()
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        repo.jobs[job["id"]].update(status="processing", locked_until="expired", attempts=1, campaign_run_id=55)
        summary = await service.recover_stale_jobs(worker_id="w1")
        assert summary["needs_review"] == 1
        assert repo.jobs[job["id"]]["status"] == "needs_review"
    asyncio.run(_run())


def test_campaign_run_id_is_persisted_before_send_completes():
    async def _run():
        repo, runtime, service = make_service(FakeRuntime(raises=True, creates_run_before_raise=True))
        job = service.enqueue_manual_launch(rule_id=4, admin_id=10, progress_chat_id=None, progress_message_id=None).job
        leased = repo.lease_repost_campaign_launch_job(job["id"], "w1", 60)
        await service.run_once(job=leased, worker_id="w1")
        stored = repo.jobs[job["id"]]
        assert runtime.launch_calls == 1
        assert stored["campaign_run_id"] == 77
        assert stored["status"] == "needs_review"
    asyncio.run(_run())


def test_source_guards_keep_force_plumbing_out_of_schedule_layers():
    handlers_source = Path("app/repost_campaign_handlers.py").read_text()
    for required in (
        "rule_repost_campaign_launch_confirm_force",
        "force_ignore_clean_channel",
        "build_repost_campaign_launch_clean_channel_warning_view",
        "build_manual_launch_policy_state",
    ):
        assert required in handlers_source

    schedule_source = Path("app/repost_campaign_schedule_service.py").read_text()
    for forbidden in (
        "force_ignore_clean_channel",
        "build_manual_launch_policy_state",
    ):
        assert forbidden not in schedule_source

    scheduled_post_source = Path("app/repost_campaign_scheduled_post_service.py").read_text()
    for forbidden in (
        "force_ignore_clean_channel",
        "build_manual_launch_policy_state",
        "RepostCampaignPlacementService",
        "waiting_clean_channel",
    ):
        assert forbidden not in scheduled_post_source
