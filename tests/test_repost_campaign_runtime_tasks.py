import asyncio
import logging
from types import SimpleNamespace

import pytest

from app.repost_campaign_runtime_tasks import RepostCampaignRuntimeTasks


def test_bot_does_not_start_repost_campaign_loops_directly():
    source = open("bot.py", encoding="utf-8").read()
    forbidden = [
        "run_repost_campaign_delete_loop",
        "run_repost_campaign_scheduled_launch_loop",
        "run_repost_campaign_scheduled_post_loop",
        "run_repost_campaign_launch_job_loop",
    ]
    for name in forbidden:
        assert name not in source


def test_repost_campaign_runtime_tasks_start_is_idempotent_and_stop_cancels(monkeypatch):
    async def _run():
        started = []
        stopped = []

        async def fake_loop(*, runtime=None, service=None, interval_seconds=0, batch_limit=None, worker_id=None, stop_event=None):
            started.append((runtime or service, interval_seconds, batch_limit, worker_id))
            await stop_event.wait()
            stopped.append(worker_id or "delete")

        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_delete_loop", fake_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_launch_loop", fake_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_post_loop", fake_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_launch_job_loop", fake_loop)

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
        assert len(manager._tasks) == 4
        assert len(started) == 4

        manager.start()
        await asyncio.sleep(0)
        assert len(manager._tasks) == 4
        assert len(started) == 4

        await manager.stop()
        assert not manager.is_running()
        assert manager._tasks == []
        assert len(stopped) == 4

    asyncio.run(_run())


def test_repost_campaign_runtime_tasks_logs_task_exception(monkeypatch, caplog):
    async def _run():
        async def failing_loop(**kwargs):
            raise RuntimeError("boom")

        async def waiting_loop(**kwargs):
            await kwargs["stop_event"].wait()

        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_delete_loop", failing_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_launch_loop", waiting_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_post_loop", waiting_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_launch_job_loop", waiting_loop)

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


def test_repost_campaign_runtime_tasks_start_cleans_up_partial_tasks_on_failure(monkeypatch, caplog):
    async def _run():
        cancelled = asyncio.Event()
        created_task = None
        create_calls = 0

        async def long_running_loop(**kwargs):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_delete_loop", long_running_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_launch_loop", long_running_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_scheduled_post_loop", long_running_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.run_repost_campaign_launch_job_loop", long_running_loop)

        manager = RepostCampaignRuntimeTasks(
            repo=SimpleNamespace(),
            bot=SimpleNamespace(),
            settings=SimpleNamespace(temp_dir="media/temp"),
            logger_=logging.getLogger("test.repost_campaign_runtime_tasks.start_failure"),
            role="bot",
        )

        def fail_on_second_create(name, coro):
            nonlocal created_task, create_calls
            create_calls += 1
            if create_calls == 2:
                coro.close()
                raise RuntimeError("start failed")
            created_task = asyncio.create_task(coro, name=name)
            manager._tasks.append(created_task)
            return created_task

        monkeypatch.setattr(manager, "_create_task", fail_on_second_create)

        with caplog.at_level(logging.ERROR, logger=manager.logger.name):
            with pytest.raises(RuntimeError, match="start failed"):
                manager.start()

        assert manager._tasks == []
        assert created_task is not None
        await asyncio.sleep(0)
        assert created_task.cancelled() or created_task.done()
        assert cancelled.is_set() or created_task.cancelled()
        assert "REPOST_CAMPAIGN_RUNTIME_TASKS_START_FAILED" in caplog.text
        assert "start failed" in caplog.text

    asyncio.run(_run())


def test_campaign_shutdown_waits_for_inflight_rpc_without_cancelling(monkeypatch):
    async def _run():
        entered = asyncio.Event()
        release = asyncio.Event()
        completed = []

        async def fake_loop(*, stop_event=None, **kwargs):
            entered.set()
            await release.wait()
            completed.append(True)
            await stop_event.wait()

        for loop in (
            "run_repost_campaign_delete_loop",
            "run_repost_campaign_scheduled_launch_loop",
            "run_repost_campaign_scheduled_post_loop",
            "run_repost_campaign_launch_job_loop",
        ):
            monkeypatch.setattr("app.repost_campaign_runtime_tasks." + loop, fake_loop)

        manager = RepostCampaignRuntimeTasks(
            repo=SimpleNamespace(), bot=SimpleNamespace(),
            settings=SimpleNamespace(temp_dir="media/temp"),
        )
        manager.start()
        await entered.wait()
        await asyncio.sleep(0)
        stopping = asyncio.create_task(manager.stop(timeout_sec=1.0))
        await asyncio.sleep(0)
        assert not stopping.done()
        release.set()
        await stopping
        assert len(completed) == 4
        assert not manager.is_running()

    asyncio.run(_run())


def test_campaign_delete_failure_resolution_warns_about_unverified_placements():
    from app.repost_campaign_ui import build_repost_campaign_run_delete_failures_resolve_confirm_view

    text, _ = build_repost_campaign_run_delete_failures_resolve_confirm_view(
        rule_id=1, run_id=2,
        details={
            "summary": {"delete_pending": 0, "delete_processing": 0},
            "messages": [{"publication_review_required": True, "delete_status": "failed", "send_status": "failed"}],
        },
    )
    assert "без подтверждённых ID" in text
    assert "вручную проверьте" in text
    assert "Публикации в Telegram удаляться не будут" in text
    assert "не разблокирует неподтверждённые отправки" in text
    assert "снимет блокировку проблемных размещений" not in text


def test_campaign_background_renderer_uses_existing_bot_transport_guard():
    raw_bot = SimpleNamespace()
    manager = RepostCampaignRuntimeTasks(
        repo=SimpleNamespace(), bot=raw_bot,
        settings=SimpleNamespace(temp_dir="media/temp"),
    )
    runtime = manager._build_campaign_runtime()
    assert runtime.renderer.bot.raw is raw_bot
    assert runtime.renderer.bot._is_transport_proxy is True
    assert runtime.deleter.bot is raw_bot


def test_campaign_shutdown_keeps_ownership_when_drain_cannot_finish(monkeypatch):
    async def _run():
        entered = asyncio.Event()
        release = asyncio.Event()
        started = []

        async def fake_loop(*, stop_event=None, **kwargs):
            started.append(True)
            entered.set()
            await release.wait()
            await stop_event.wait()

        async def incomplete_drain(*, tasks, stop_event, timeout_sec, logger):
            stop_event.set()  # An in-flight Telegram RPC did not finish.

        for name in (
            "run_repost_campaign_delete_loop",
            "run_repost_campaign_scheduled_launch_loop",
            "run_repost_campaign_scheduled_post_loop",
            "run_repost_campaign_launch_job_loop",
        ):
            monkeypatch.setattr("app.repost_campaign_runtime_tasks." + name, fake_loop)
        monkeypatch.setattr("app.repost_campaign_runtime_tasks.drain_job_workers", incomplete_drain)

        manager = RepostCampaignRuntimeTasks(
            repo=SimpleNamespace(), bot=SimpleNamespace(),
            settings=SimpleNamespace(temp_dir="media/temp"),
        )
        manager.start()
        await entered.wait()
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="нельзя закрывать"):
            await manager.stop(timeout_sec=0.01)
        assert manager.is_running()
        assert len(manager._tasks) == 4
        manager.start()
        assert len(started) == 4
        assert manager._stop_event.is_set()
        release.set()
        await asyncio.gather(*manager._tasks)
        assert not manager.is_running()
        await manager.stop()
        assert manager._tasks == []

    asyncio.run(_run())


def test_campaign_delete_confirmation_handles_missing_or_null_message_rows():
    from app.repost_campaign_ui import build_repost_campaign_run_delete_failures_resolve_confirm_view

    for messages in (None, [None], [None, {"publication_review_required": False, "delete_status": "failed"}]):
        message, keyboard = build_repost_campaign_run_delete_failures_resolve_confirm_view(
            rule_id=1, run_id=2, details={"messages": messages, "summary": {}}
        )
        assert "Снять проблему удаления" in message
        assert "без подтверждённых ID" not in message
        assert keyboard is not None


def test_bot_polling_preserves_session_until_campaign_shutdown():
    import ast
    from pathlib import Path

    source = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    role = next(
        node for node in source.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_start_bot_role"
    )
    polling_calls = [
        node for node in ast.walk(role)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "start_polling"
    ]
    assert len(polling_calls) == 1
    session_options = [
        kw.value for kw in polling_calls[0].keywords if kw.arg == "close_bot_session"
    ]
    assert len(session_options) == 1
    assert isinstance(session_options[0], ast.Constant)
    assert session_options[0].value is False


def test_incomplete_campaign_drain_retries_before_closing_telegram_clients():
    import ast
    from pathlib import Path

    parsed = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    fn = next(
        n for n in parsed.body
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_shutdown_runtime"
    )
    events = []

    async def fail_drain():
        events.append("campaign_stop")
        if events.count("campaign_stop") == 1:
            raise RuntimeError("in-flight Telegram RPC")
        assert "telethon_disconnect" not in events
        assert "bot_session_close" not in events

    async def record(name):
        events.append(name)

    campaign = SimpleNamespace(stop=fail_drain)
    raw_telethon = SimpleNamespace(disconnect=lambda: record("telethon_disconnect"))
    reactor = SimpleNamespace(client=SimpleNamespace(disconnect=lambda: record("reaction_disconnect")), session_name="r")
    raw_bot = SimpleNamespace(session=SimpleNamespace(close=lambda: record("bot_session_close")))
    ctx = {
        "__name__": "test.shutdown",
        "asyncio": asyncio, "logger": logging.getLogger("test.shutdown.drain"),
        "stop_background_tasks": lambda: record("background_cleanup"),
        "stop_job_workers_runtime": lambda: record("workers_cleanup"),
        "runtime_ownership": SimpleNamespace(begin_shutdown=lambda: None),
        "stop_all_workers": lambda: record("all_workers_cleanup"),
        "dashboard_tasks": {},
        "channel_title_sync_task": None,
        "repost_campaign_runtime_tasks": campaign,
        "scheduler_runtime_task": None,
        "rescan_recovery_task": None,
        "rule_ui_tasks": {},
        "job_watchdog_task": None,
        "reaction_worker_task": None,
        "sender_service": SimpleNamespace(video_processor=SimpleNamespace(
            shutdown=lambda: record("processor_cleanup"),
        )),
        "telethon_client": raw_telethon,
        "reaction_clients": [reactor],
        "bot": raw_bot,
        "runtime_context": SimpleNamespace(),
    }
    module = ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[]))
    exec(compile(module, "bot.py", "exec"), ctx)
    result = asyncio.run(ctx["_shutdown_runtime"](
        stop_workers_runtime=False, close_dashboard_tasks=True,
        close_telegram_clients=True, close_bot_session=True,
    ))
    assert result is True
    assert events.count("campaign_stop") == 2
    assert "background_cleanup" in events and "processor_cleanup" in events
    assert events.index("telethon_disconnect") > events.index("campaign_stop")
    assert "reaction_disconnect" in events
    assert "bot_session_close" in events
    assert ctx["repost_campaign_runtime_tasks"] is None
    assert ctx["telethon_client"] is None
    assert ctx["bot"] is None


def test_main_preserves_original_role_error_when_shutdown_is_incomplete():
    import ast
    from pathlib import Path

    parsed = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    fn = next(
        n for n in parsed.body
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "main"
    )

    async def role_failure(*args, **kwargs):
        raise ValueError("original role error")

    async def incomplete_shutdown(**kwargs):
        return False

    ctx = {
        "__name__": "test.main_shutdown",
        "normalize_runtime_role": lambda role: role,
        "logger": logging.getLogger("test.main_shutdown"),
        "run_preflight_checks": lambda *args, **kwargs: SimpleNamespace(messages=[]),
        "PreflightError": RuntimeError,
        "settings": SimpleNamespace(),
        "run_runtime_role": role_failure,
        "_start_bot_role": lambda: None,
        "_start_scheduler_role": lambda: None,
        "_start_worker_role": lambda: None,
        "_start_all_role": lambda: None,
        "_shutdown_runtime": incomplete_shutdown,
    }
    module = ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[]))
    exec(compile(module, "bot.py", "exec"), ctx)
    with pytest.raises(ValueError, match="original role error"):
        asyncio.run(ctx["main"]("bot"))
