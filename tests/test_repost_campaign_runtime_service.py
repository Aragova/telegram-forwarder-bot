import asyncio
from types import SimpleNamespace

from app.repost_campaign_runtime_service import RepostCampaignRuntimeService
from app.saved_post_renderer import SavedPostRenderResult


class _FakeRepo:
    def __init__(self, *, rule=None, saved_post=None, summary=None):
        self._rule = rule
        self._saved_post = saved_post
        self._summary = summary or {}
        self.create_campaign_run_calls = []
        self.update_campaign_run_status_calls = []
        self.create_campaign_run_message_calls = []
        self.mark_campaign_run_message_sending_calls = []
        self.mark_campaign_run_message_sent_calls = []
        self.mark_campaign_run_message_failed_calls = []
        self.next_run_id = 101
        self.next_run_message_id = 1001
        self._runs = []
        self._run = None
        self._messages = []
        self._targets = []
        self._due_delete_rows = []
        self.claim_due_campaign_run_messages_for_delete_calls = []
        self.mark_campaign_run_message_deleted_calls = []
        self.mark_campaign_run_message_delete_failed_calls = []
        self.reset_stuck_campaign_delete_processing_calls = []

    def get_rule(self, rule_id):
        return self._rule

    def get_saved_post(self, saved_post_id):
        return self._saved_post

    def get_rule_repost_campaign_summary(self, rule_id):
        return self._summary

    def create_campaign_run(self, **kwargs):
        self.create_campaign_run_calls.append(kwargs)
        return self.next_run_id

    def update_campaign_run_status(self, run_id, **kwargs):
        self.update_campaign_run_status_calls.append((run_id, kwargs))
        return True

    def create_campaign_run_message(self, **kwargs):
        self.create_campaign_run_message_calls.append(kwargs)
        return self.next_run_message_id

    def mark_campaign_run_message_sending(self, message_id, **kwargs):
        self.mark_campaign_run_message_sending_calls.append((message_id, kwargs))
        return True

    def mark_campaign_run_message_sent(self, message_id, **kwargs):
        self.mark_campaign_run_message_sent_calls.append((message_id, kwargs))
        return True

    def mark_campaign_run_message_failed(self, message_id, **kwargs):
        self.mark_campaign_run_message_failed_calls.append((message_id, kwargs))
        return True

    def list_campaign_runs_for_rule(self, rule_id, limit=10):
        return self._runs[:limit]

    def get_campaign_run(self, run_id):
        return self._run

    def list_campaign_run_messages(self, run_id):
        return self._messages

    def list_rule_repost_campaign_targets(self, rule_id, active_only=True):
        return list(self._targets)

    def claim_due_campaign_run_messages_for_delete(self, *, limit=50):
        self.claim_due_campaign_run_messages_for_delete_calls.append(limit)
        return list(self._due_delete_rows)

    def mark_campaign_run_message_deleted(self, message_id):
        self.mark_campaign_run_message_deleted_calls.append(message_id)
        return True

    def mark_campaign_run_message_delete_failed(self, message_id, *, error_text):
        self.mark_campaign_run_message_delete_failed_calls.append((message_id, error_text))
        return True

    def reset_stuck_campaign_delete_processing(self, *, stuck_seconds=300):
        self.reset_stuck_campaign_delete_processing_calls.append(stuck_seconds)
        return 0


class _FakeRenderer:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self.result, list):
            return self.result[len(self.calls) - 1]
        return self.result


def test_preview_fail_no_rule():
    repo = _FakeRepo(rule=None)
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = asyncio.run(runtime.preview_saved_post(rule_id=1, admin_chat_id=123))
    assert result.ok is False
    assert result.error_text == "Правило не найдено"


def test_test_send_fail_no_saved_post():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=None, target_id="-1001")
    repo = _FakeRepo(rule=rule)
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = asyncio.run(runtime.test_send_saved_post_to_main_target(rule_id=2))
    assert result.ok is False
    assert result.error_text == "Рекламный пост не выбран"


def test_test_send_success():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1001")
    saved_post = {"content_json": {"kind": "photo", "media": {"file_id": "x"}}}
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="photo", chat_id="-1001", message_id=123))
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=rule, saved_post=saved_post), renderer=renderer)
    result = asyncio.run(runtime.test_send_saved_post_to_main_target(rule_id=3))
    assert result.ok is True
    assert result.message_id == 123
    assert result.method == "bot_api"
    assert result.target_id == "-1001"


def test_send_creates_run_and_message_on_success():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1001", repost_campaign_show_seconds=300)
    saved_post = {"content_json": {"kind": "photo"}}
    repo = _FakeRepo(rule=rule, saved_post=saved_post)
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="photo", chat_id="-1001", message_id=123))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)

    result = asyncio.run(runtime.test_send_saved_post_to_main_target(rule_id=33, admin_id=777))

    assert len(repo.create_campaign_run_calls) == 1
    assert len(repo.create_campaign_run_message_calls) == 1
    assert len(repo.mark_campaign_run_message_sending_calls) == 1
    assert len(repo.mark_campaign_run_message_sent_calls) == 1
    assert repo.update_campaign_run_status_calls[-1][1]["status"] == "sent"
    assert result.extra["campaign_run_id"] == repo.next_run_id


def test_send_marks_run_failed_on_renderer_error():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1001", repost_campaign_show_seconds=300)
    saved_post = {"content_json": {"kind": "photo"}}
    repo = _FakeRepo(rule=rule, saved_post=saved_post)
    renderer = _FakeRenderer(SavedPostRenderResult(ok=False, method="telethon_builder", kind="photo", error_text="Telethon error"))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)

    result = asyncio.run(runtime.test_send_saved_post_to_main_target(rule_id=34))
    assert len(repo.mark_campaign_run_message_failed_calls) == 1
    assert repo.update_campaign_run_status_calls[-1][1]["status"] == "failed"
    assert result.ok is False
    assert result.extra["campaign_run_id"] == repo.next_run_id


def test_send_does_not_render_if_run_creation_failed():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1001", repost_campaign_show_seconds=300)
    saved_post = {"content_json": {"kind": "photo"}}
    repo = _FakeRepo(rule=rule, saved_post=saved_post)
    repo.next_run_id = None
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="photo", chat_id="-1001", message_id=123))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)

    result = asyncio.run(runtime.test_send_saved_post_to_main_target(rule_id=35))
    assert len(renderer.calls) == 0
    assert result.ok is False


def test_test_send_premium_failure_propagated():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1001")
    saved_post = {"content_json": {"kind": "photo", "media": {"file_id": "x"}}}
    renderer = _FakeRenderer(
        SavedPostRenderResult(
            ok=False,
            method="telethon_builder",
            kind="photo",
            error_text="Telethon error",
            premium_required=True,
        )
    )
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=rule, saved_post=saved_post), renderer=renderer)
    result = asyncio.run(runtime.test_send_saved_post_to_main_target(rule_id=4))
    assert result.ok is False
    assert result.premium_required is True
    assert result.error_text == "Telethon error"


def test_readiness_ready_status():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=11, repost_campaign_show_seconds=43200)
    repo = _FakeRepo(rule=rule, summary={"targets_active": 2, "targets_with_errors": 0})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.get_campaign_readiness(rule_id=5)
    assert result["ready"] is True
    assert result["status"] == "ready"
    assert "✅ выбран" in result["post_status_text"]
    assert "✅ 12 часов" in result["show_seconds_status_text"]


def test_readiness_warning_status():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=None, repost_campaign_show_seconds=0)
    repo = _FakeRepo(rule=rule, summary={"targets_active": 0, "targets_with_errors": 2})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.get_campaign_readiness(rule_id=6)
    assert result["ready"] is False
    assert result["status"] == "warning"
    assert "Рекламный пост не выбран" in result["warnings"]
    assert "Срок показа не задан" in result["warnings"]
    assert "Активных каналов кампании пока нет" in result["warnings"]
    assert "Есть каналы, которые требуют проверки: 2" in result["warnings"]


def test_get_campaign_history_no_rule():
    repo = _FakeRepo(rule=None)
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    history = runtime.get_campaign_history(rule_id=11)
    assert history["ok"] is False
    assert history["error_text"] == "Правило не найдено"


def test_get_campaign_history_summary_counts():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._runs = [{"status": "sent"}, {"status": "failed"}, {"status": "partial"}, {"status": "sending"}]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    history = runtime.get_campaign_history(rule_id=11)
    assert history["summary"]["total"] == 4
    assert history["summary"]["sent"] == 1
    assert history["summary"]["failed"] == 1
    assert history["summary"]["partial"] == 1
    assert history["summary"]["sending"] == 1


def test_get_campaign_run_details_wrong_rule():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 10, "rule_id": 2}
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    details = runtime.get_campaign_run_details(rule_id=1, run_id=10)
    assert details["ok"] is False
    assert details["error_text"] == "Запуск не относится к этому правилу"


def test_get_campaign_run_details_success():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 10, "rule_id": 1}
    repo._messages = [
        {"send_status": "sent", "delete_status": "pending"},
        {"send_status": "failed", "delete_status": "deleted"},
        {"send_status": "sending", "delete_status": "failed"},
    ]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    details = runtime.get_campaign_run_details(rule_id=1, run_id=10)
    assert details["ok"] is True
    assert details["summary"]["total"] == 3
    assert details["summary"]["sent"] == 1
    assert details["summary"]["failed"] == 1
    assert details["summary"]["pending"] == 1
    assert details["summary"]["delete_pending"] == 1
    assert details["summary"]["deleted"] == 1
    assert details["summary"]["delete_failed"] == 1


def test_launch_main_only_success():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=43200, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "text"}})
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1001", message_id=1))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1, admin_id=1))
    assert result.ok is True
    assert result.extra["targets_total"] == 1
    assert result.extra["targets_success"] == 1
    assert result.extra["final_status"] == "sent"
    assert repo.create_campaign_run_message_calls[0]["target_kind"] == "main"


def test_process_due_deletions_no_deleter():
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(), renderer=_FakeRenderer(None), deleter=None)
    result = asyncio.run(runtime.process_due_deletions())
    assert result["ok"] is False
    assert result["error_text"] == "Delete service недоступен"


def test_process_due_deletions_no_due_rows():
    repo = _FakeRepo()
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_message=None))
    async def _noop(**kwargs):  # pragma: no cover
        return None
    runtime.deleter.delete_message = _noop
    result = asyncio.run(runtime.process_due_deletions())
    assert result["claimed"] == 0
    assert result["deleted"] == 0
    assert result["failed"] == 0


def test_process_due_deletions_success():
    repo = _FakeRepo()
    repo._due_delete_rows = [{"id": 3, "target_id": "-1002741117827", "sent_message_id": 2466, "render_mode": "telethon_builder"}]
    async def _ok(**kwargs):
        return SimpleNamespace(ok=True, method="telethon", error_text=None)
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_message=_ok))
    result = asyncio.run(runtime.process_due_deletions())
    assert repo.mark_campaign_run_message_deleted_calls == [3]
    assert result["deleted"] == 1
    assert result["failed"] == 0


def test_process_due_deletions_failed():
    repo = _FakeRepo()
    repo._due_delete_rows = [{"id": 4, "target_id": "-1002741117827", "sent_message_id": 2467, "render_mode": None}]
    async def _fail(**kwargs):
        return SimpleNamespace(ok=False, method="failed", error_text="no rights")
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_message=_fail))
    result = asyncio.run(runtime.process_due_deletions())
    assert repo.mark_campaign_run_message_delete_failed_calls[0][0] == 4
    assert result["failed"] == 1


def test_launch_main_plus_extras_success():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=43200, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "text"}})
    repo._targets = [{"target_id": "-1002", "title": "A"}, {"target_id": "-1003", "title": "B"}]
    renderer = _FakeRenderer([
        SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1001", message_id=1),
        SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1002", message_id=2),
        SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1003", message_id=3),
    ])
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.extra["targets_total"] == 3
    assert result.extra["targets_success"] == 3
    assert len(repo.create_campaign_run_message_calls) == 3


def test_launch_partial():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=43200, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "text"}})
    repo._targets = [{"target_id": "-1002", "title": "A"}]
    renderer = _FakeRenderer([
        SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1001", message_id=1),
        SavedPostRenderResult(ok=False, method="telethon_builder", kind="text", error_text="x"),
    ])
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.extra["final_status"] == "partial"
    assert result.extra["targets_success"] == 1
    assert result.extra["targets_failed"] == 1
    assert result.ok is True


def test_launch_all_failed():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=43200, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "text"}})
    repo._targets = [{"target_id": "-1002", "title": "A"}]
    renderer = _FakeRenderer([
        SavedPostRenderResult(ok=False, method="bot_api", kind="text", error_text="e1"),
        SavedPostRenderResult(ok=False, method="telethon_builder", kind="text", error_text="e2"),
    ])
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.extra["final_status"] == "failed"
    assert result.ok is False
    assert result.extra["targets_success"] == 0
    assert result.extra["targets_failed"] == 2


def test_launch_dedupe_main_and_extra():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=43200, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "text"}})
    repo._targets = [{"target_id": "-1001", "title": "dup"}]
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1001", message_id=1))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.extra["targets_total"] == 1
    assert len(repo.create_campaign_run_message_calls) == 1


def test_launch_no_show_seconds():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=0, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "text"}})
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1001", message_id=1))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.ok is False
    assert result.error_text == "Срок показа не задан"
    assert len(renderer.calls) == 0
