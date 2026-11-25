"""Regressions for the PR #525 final publication/campaign safety audit."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from app.publication_mapping_spool import PublicationMappingSpool
from app.telegram_publication import verified_published_message
from app.send_intent import PublicationWriteUnavailable, _definite_rejection
from app.repost_campaign_launch_job_service import RepostCampaignLaunchJobService
from app.repost_campaign_runtime_service import RepostCampaignRuntimeService


def test_publication_mapping_spool_survives_restart_and_replays_exact_members(tmp_path):
    path = tmp_path / "sender.publication-mappings.sqlite"
    first = PublicationMappingSpool(path)
    first.append(-100123, [11, 12], [201, 202])
    first.append(-100123, [13], [203])

    restarted = PublicationMappingSpool(path)
    rows = restarted.pending()
    assert [(peer, old, new) for _, peer, old, new in rows] == [
        (-100123, [11, 12], [201, 202]),
        (-100123, [13], [203]),
    ]
    restarted.acknowledge(rows[0][0])
    assert [(old, new) for _, _, old, new in PublicationMappingSpool(path).pending()] == [
        ([13], [203]),
    ]
    restarted.acknowledge(rows[1][0])
    assert PublicationMappingSpool(path).pending() == []


def test_spool_rejects_incomplete_mapping_before_persist(tmp_path):
    spool = PublicationMappingSpool(tmp_path / "sender.sqlite")
    try:
        spool.append(-100123, [11, 12], [201])
    except ValueError:
        pass
    else:
        raise AssertionError("incomplete publication mappings must be rejected")
    assert spool.pending() == []


def test_general_topic_uses_absent_reply_header_as_thread_one():
    now = datetime.now(timezone.utc)
    message = SimpleNamespace(
        id=501, chat_id=-100123, post=True, out=True,
        from_scheduled=True, video_processing_pending=False,
        reply_to=None, date=now,
    )
    kwargs = dict(
        peer_id=-100123, published_id=501, source_ids={1, 2},
        before_id=100, started_at=now - timedelta(seconds=2),
        broadcast=True,
    )
    assert verified_published_message(message, thread_id=1, **kwargs)
    assert not verified_published_message(message, thread_id=321, **kwargs)


def test_publication_write_unavailable_is_definitely_before_wire():
    assert _definite_rejection(PublicationWriteUnavailable("inbox is blocked"))


def test_campaign_stale_lease_must_not_enter_launch():
    async def scenario():
        repo = SimpleNamespace(
            mark_repost_campaign_launch_job_processing=Mock(return_value=None),
        )
        launch = SimpleNamespace(launch_campaign_now=AsyncMock())
        service = RepostCampaignLaunchJobService.__new__(RepostCampaignLaunchJobService)
        service.repo = repo
        service.campaign_runtime = launch
        service.logger = Mock()
        assert await service.run_once(
            job={"id": 21, "rule_id": 7, "admin_id": 1, "payload_json": {}},
            worker_id="old-worker",
        ) is None
        launch.launch_campaign_now.assert_not_awaited()
        repo.mark_repost_campaign_launch_job_processing.assert_called_once()

    asyncio.run(scenario())


def test_manual_campaign_delete_during_sending_cannot_forge_deleted_status():
    async def scenario():
        repo = SimpleNamespace(
            get_campaign_run=Mock(return_value={"id": 21, "rule_id": 7}),
            get_campaign_run_message=Mock(return_value={
                "id": 33, "run_id": 21, "rule_id": 7,
                "send_status": "sending", "delete_status": "pending",
            }),
            mark_campaign_run_message_deleted=Mock(),
        )
        service = RepostCampaignRuntimeService.__new__(RepostCampaignRuntimeService)
        service.repo = repo
        service.deleter = SimpleNamespace()
        service.logger = Mock()
        result = await service.delete_campaign_run_message_now(
            rule_id=7, run_id=21, run_message_id=33,
        )
        assert not result.ok
        repo.mark_campaign_run_message_deleted.assert_not_called()

    asyncio.run(scenario())


def test_album_and_campaign_race_fences_are_present_at_write_boundary():
    # Guard the exact concurrency contracts against accidental refactors.
    root = Path(__file__).resolve().parents[1]
    sql = (root / "app/postgres_repository.py").read_text(encoding="utf-8")
    worker = (root / "app/worker_runtime.py").read_text(encoding="utf-8")
    sender = (root / "app/send_intent.py").read_text(encoding="utf-8")
    assert "AND locked_by=%s AND locked_until > NOW()" in sql
    assert "claim_campaign_scheduled_post_snapshot" in sql
    assert "FOR UPDATE OF d" in sql
    assert "quarantine_queued_self_target_echo_deliveries" in worker
    assert "async def publish_fenced()" in sender
    assert 'getattr(self, "publication_write_blocked", False)' in sender


def test_billing_db_access_offloaded_from_async_worker():
    source = (Path(__file__).resolve().parents[1] / "app/worker_runtime.py").read_text(
        encoding="utf-8",
    )
    assert "await asyncio.to_thread(repo.create_billing_event," in source
    assert "await asyncio.to_thread(subscription_service.get_active_subscription," in source
    assert "await asyncio.to_thread(usage_service.get_today_usage," in source
