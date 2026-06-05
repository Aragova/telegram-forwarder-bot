import asyncio
import logging
from types import SimpleNamespace

from app.repost_campaign_runtime_tasks import RepostCampaignRuntimeTasks


def test_bot_does_not_start_repost_campaign_loops_directly():
    source = open("bot.py", encoding="utf-8").read()
    forbidden = [
        "run_repost_campaign_delete_loop",
        "run_repost_campaign_scheduled_launch_loop",
        "run_repost_campaign_scheduled_post_loop",
    ]
    for name in forbidden:
        assert name not in source


def test_repost_campaign_runtime_tasks_start_is_idempotent_and_stop_cancels(monkeypatch):
    async def _run():
        started = []
        cancelled = []

        async def fake_loop(*, runtime, interval_seconds=0, batch_limit=None, worker_id=None):
            started.append((runtime, interval_seconds, batch_limit, worker_id))
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(worker_id or "delete")
                raise

        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_delete_loop", fake_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_launch_loop", fake_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_post_loop", fake_loop)

        manager = RepostCampaignRuntimeTasks(
            repo=SimpleNamespace(),
            bot=SimpleNamespace(),
            telethon_client=SimpleNamespace(),
            settings=SimpleNamespace(temp_dir="media/temp"),
            logger_=logging.getLogger("test.repost_campaign_runtime_tasks"),
            role="bot",
        )

        manager.start()
        await asyncio.sleep(0)
        assert manager.is_running()
        assert len(manager._tasks) == 3
        assert len(started) == 3

        manager.start()
        await asyncio.sleep(0)
        assert len(manager._tasks) == 3
        assert len(started) == 3

        await manager.stop()
        assert not manager.is_running()
        assert manager._tasks == []
        assert len(cancelled) == 3

    asyncio.run(_run())


def test_repost_campaign_runtime_tasks_logs_task_exception(monkeypatch, caplog):
    async def _run():
        async def failing_loop(**kwargs):
            raise RuntimeError("boom")

        async def waiting_loop(**kwargs):
            await asyncio.Event().wait()

        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_delete_loop", failing_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_launch_loop", waiting_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_post_loop", waiting_loop)

        logger = logging.getLogger("test.repost_campaign_runtime_tasks.exception")
        manager = RepostCampaignRuntimeTasks(
            repo=SimpleNamespace(),
            bot=SimpleNamespace(),
            settings=SimpleNamespace(temp_dir="media/temp"),
            logger_=logger,
            role="bot",
        )

        with caplog.at_level(logging.ERROR, logger=logger.name):
            manager.start()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert "REPOST_CAMPAIGN_RUNTIME_TASK_FAILED" in caplog.text
            assert "repost_campaign_delete_loop" in caplog.text
            assert "boom" in caplog.text
        await manager.stop()

    asyncio.run(_run())
