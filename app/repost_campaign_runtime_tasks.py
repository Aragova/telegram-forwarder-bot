from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.repost_campaign_delete_service import RepostCampaignDeleteService, run_repost_campaign_delete_loop
from app.repost_campaign_runtime_service import RepostCampaignRuntimeService
from app.repost_campaign_schedule_service import RepostCampaignScheduleService, run_repost_campaign_scheduled_launch_loop
from app.repost_campaign_scheduled_post_service import RepostCampaignScheduledPostService, run_repost_campaign_scheduled_post_loop
from app.repost_campaign_target_check_service import RepostCampaignTargetCheckService
from app.saved_post_renderer import SavedPostRenderer


class RepostCampaignRuntimeTasks:
    """Управляет lifecycle фоновых runtime-задач рекламной кампании."""

    def __init__(
        self,
        *,
        repo: Any,
        bot: Any,
        telethon_client: Any = None,
        settings: Any = None,
        logger_: logging.Logger | None = None,
        enabled: bool = True,
        role: str = "bot",
        delete_interval_seconds: int = 10,
        delete_batch_limit: int = 50,
        scheduled_launch_interval_seconds: int = 15,
        scheduled_post_interval_seconds: int = 15,
    ) -> None:
        self.repo = repo
        self.bot = bot
        self.telethon_client = telethon_client
        self.settings = settings
        self.logger = logger_ or logging.getLogger("forwarder")
        self.enabled = bool(enabled)
        self.role = role
        self.delete_interval_seconds = int(delete_interval_seconds)
        self.delete_batch_limit = int(delete_batch_limit)
        self.scheduled_launch_interval_seconds = int(scheduled_launch_interval_seconds)
        self.scheduled_post_interval_seconds = int(scheduled_post_interval_seconds)
        self._tasks: list[asyncio.Task] = []

    def is_running(self) -> bool:
        return any(not task.done() for task in self._tasks)

    def start(self) -> None:
        if not self.enabled:
            self.logger.info("REPOST_CAMPAIGN_RUNTIME_TASKS_DISABLED")
            return
        self._tasks = [task for task in self._tasks if not task.done()]
        if self._tasks:
            self.logger.info("REPOST_CAMPAIGN_RUNTIME_TASKS_ALREADY_RUNNING | tasks=%s", len(self._tasks))
            return

        campaign_runtime = self._build_campaign_runtime()
        schedule_runtime = RepostCampaignScheduleService(repo=self.repo, campaign_runtime=campaign_runtime, logger_=self.logger)
        scheduled_post_runtime = self._build_scheduled_post_runtime(campaign_runtime)

        self._create_task(
            "repost_campaign_delete_loop",
            run_repost_campaign_delete_loop(
                runtime=campaign_runtime,
                interval_seconds=self.delete_interval_seconds,
                batch_limit=self.delete_batch_limit,
            ),
        )
        self._create_task(
            "repost_campaign_scheduled_launch_loop",
            run_repost_campaign_scheduled_launch_loop(
                runtime=schedule_runtime,
                interval_seconds=self.scheduled_launch_interval_seconds,
                worker_id=f"campaign-schedule:{self.role}",
            ),
        )
        self._create_task(
            "repost_campaign_scheduled_post_loop",
            run_repost_campaign_scheduled_post_loop(
                runtime=scheduled_post_runtime,
                interval_seconds=self.scheduled_post_interval_seconds,
                worker_id=f"vip-scheduled-post:{self.role}",
            ),
        )
        self.logger.info(
            "REPOST_CAMPAIGN_RUNTIME_TASKS_STARTED | tasks=%s | delete_interval_seconds=%s | delete_batch_limit=%s",
            len(self._tasks),
            self.delete_interval_seconds,
            self.delete_batch_limit,
        )

    async def stop(self) -> None:
        if not self._tasks:
            self.logger.info("REPOST_CAMPAIGN_RUNTIME_TASKS_STOPPED | tasks=0")
            return
        tasks = list(self._tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self.logger.info("REPOST_CAMPAIGN_RUNTIME_TASKS_STOPPED | tasks=%s", len(tasks))

    def _build_campaign_runtime(self) -> RepostCampaignRuntimeService:
        renderer = SavedPostRenderer(
            bot=self.bot,
            telethon_client=self.telethon_client,
            temp_dir=getattr(self.settings, "temp_dir", "media/temp"),
            logger_=self.logger,
        )
        deleter = RepostCampaignDeleteService(
            bot=self.bot,
            telethon_client=self.telethon_client,
            logger_=self.logger,
        )
        target_checker = RepostCampaignTargetCheckService(
            telethon_client=self.telethon_client,
            bot=self.bot,
            logger_=self.logger,
        )
        return RepostCampaignRuntimeService(
            repo=self.repo,
            renderer=renderer,
            deleter=deleter,
            target_checker=target_checker,
            telethon_client=self.telethon_client,
            logger_=self.logger,
        )

    def _build_scheduled_post_runtime(self, campaign_runtime: RepostCampaignRuntimeService) -> RepostCampaignScheduledPostService:
        target_checker = RepostCampaignTargetCheckService(
            telethon_client=self.telethon_client,
            bot=self.bot,
            logger_=self.logger,
        )
        return RepostCampaignScheduledPostService(
            repo=self.repo,
            campaign_runtime=campaign_runtime,
            target_checker=target_checker,
            logger_=self.logger,
        )

    def _create_task(self, name: str, coro) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        task.add_done_callback(lambda done_task, task_name=name: self._log_task_done(task_name, done_task))
        self._tasks.append(task)
        return task

    def _log_task_done(self, name: str, task: asyncio.Task) -> None:
        if task.cancelled():
            self.logger.info("REPOST_CAMPAIGN_RUNTIME_TASK_CANCELLED | task=%s", name)
            return
        exc = task.exception()
        if exc is not None:
            self.logger.error(
                "REPOST_CAMPAIGN_RUNTIME_TASK_FAILED | task=%s | error=%s",
                name,
                exc,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            return
        self.logger.warning("REPOST_CAMPAIGN_RUNTIME_TASK_FINISHED_UNEXPECTEDLY | task=%s", name)
