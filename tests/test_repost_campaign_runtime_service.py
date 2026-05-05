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
        self._message = None
        self._targets = []
        self._due_delete_rows = []
        self.claim_due_campaign_run_messages_for_delete_calls = []
        self.mark_campaign_run_message_deleted_calls = []
        self.mark_campaign_run_message_delete_failed_calls = []
        self.reset_stuck_campaign_delete_processing_calls = []
        self.set_target_active_calls = []
        self.remove_target_calls = []
        self.update_target_check_calls = []

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
    def get_campaign_run_message(self, message_id):
        return self._message

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

    def set_rule_repost_campaign_target_active(self, target_row_id, is_active):
        self.set_target_active_calls.append((target_row_id, is_active))
        return True

    def remove_rule_repost_campaign_target(self, target_row_id):
        self.remove_target_calls.append(target_row_id)
        return True

    def update_rule_repost_campaign_target_check_result(self, row_id, *, title=None, last_check_error=None):
        self.update_target_check_calls.append((row_id, title, last_check_error))
        return True


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


def test_set_campaign_target_active_success():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._targets = [{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.set_campaign_target_active(rule_id=3, target_row_id=1, is_active=False)
    assert result["ok"] is True
    assert repo.set_target_active_calls[-1] == (1, False)


def test_set_campaign_target_active_not_found():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.set_campaign_target_active(rule_id=3, target_row_id=1, is_active=False)
    assert result["ok"] is False
    assert result["error_text"] == "Канал/группа не найдены в кампании"


def test_remove_campaign_target_success():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._targets = [{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.remove_campaign_target(rule_id=3, target_row_id=1)
    assert result["ok"] is True
    assert repo.remove_target_calls == [1]


def test_remove_campaign_target_not_found():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.remove_campaign_target(rule_id=3, target_row_id=1)
    assert result["ok"] is False


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


def test_manual_delete_success():
    repo = _FakeRepo()
    repo._run = {"id": 10, "rule_id": 3}
    repo._message = {"id": 33, "run_id": 10, "rule_id": 3, "send_status": "sent", "target_id": "-1001", "sent_message_id": 777, "render_mode": "telethon_builder", "delete_status": "failed"}
    async def _ok(**kwargs):
        return SimpleNamespace(ok=True, method="telethon", error_text=None)
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_message=_ok))
    result = asyncio.run(runtime.delete_campaign_run_message_now(rule_id=3, run_id=10, run_message_id=33))
    assert result.ok is True
    assert repo.mark_campaign_run_message_deleted_calls == [33]
    assert result.extra["campaign_run_message_id"] == 33


def test_manual_delete_already_deleted():
    repo = _FakeRepo()
    repo._run = {"id": 10, "rule_id": 3}
    repo._message = {"id": 33, "run_id": 10, "rule_id": 3, "send_status": "sent", "target_id": "-1001", "sent_message_id": 777, "delete_status": "deleted"}
    deleter = SimpleNamespace(delete_message=None)
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=deleter)
    result = asyncio.run(runtime.delete_campaign_run_message_now(rule_id=3, run_id=10, run_message_id=33))
    assert result.ok is True
    assert result.method == "already_deleted"
    assert result.extra["already_deleted"] is True


def test_manual_delete_send_status_not_sent():
    repo = _FakeRepo()
    repo._run = {"id": 10, "rule_id": 3}
    repo._message = {"id": 33, "run_id": 10, "rule_id": 3, "send_status": "failed", "target_id": "-1001", "sent_message_id": 777, "delete_status": "failed"}
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_message=lambda **kwargs: None))
    result = asyncio.run(runtime.delete_campaign_run_message_now(rule_id=3, run_id=10, run_message_id=33))
    assert result.ok is False
    assert result.error_text == "Публикация ещё не была успешно отправлена"


def test_manual_delete_failed():
    repo = _FakeRepo()
    repo._run = {"id": 10, "rule_id": 3}
    repo._message = {"id": 33, "run_id": 10, "rule_id": 3, "send_status": "sent", "target_id": "-1001", "sent_message_id": 777, "delete_status": "failed"}
    async def _fail(**kwargs):
        return SimpleNamespace(ok=False, method="failed", error_text="no rights")
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_message=_fail))
    result = asyncio.run(runtime.delete_campaign_run_message_now(rule_id=3, run_id=10, run_message_id=33))
    assert result.ok is False
    assert result.error_text == "no rights"
    assert repo.mark_campaign_run_message_delete_failed_calls[0][0] == 33


def test_manual_delete_wrong_message_ownership():
    repo = _FakeRepo()
    repo._run = {"id": 10, "rule_id": 3}
    repo._message = {"id": 33, "run_id": 11, "rule_id": 3}
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_message=lambda **kwargs: None))
    result = asyncio.run(runtime.delete_campaign_run_message_now(rule_id=3, run_id=10, run_message_id=33))
    assert result.ok is False
    assert result.error_text == "Публикация не относится к этому запуску"


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

def test_get_campaign_control_center_no_history():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=None, repost_campaign_show_seconds=0)
    repo = _FakeRepo(rule=rule, summary={"targets_active": 0, "targets_with_errors": 0})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.get_campaign_control_center(rule_id=42)
    assert result["ok"] is True
    assert result["last_run"] is None
    assert "Рекламный пост не выбран" in result["issues"]


def test_get_campaign_control_center_with_last_run_details():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost", repost_campaign_saved_post_id=11, repost_campaign_show_seconds=60), summary={"targets_active": 2, "targets_with_errors": 0})
    repo._runs = [{"id": 4, "status": "sent", "rule_id": 1}]
    repo._run = {"id": 4, "rule_id": 1}
    repo._messages = [
        {"send_status": "sent", "delete_status": "failed"},
        {"send_status": "sent", "delete_status": "pending"},
        {"send_status": "sent", "delete_status": "pending"},
    ]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.get_campaign_control_center(rule_id=1)
    assert "Ошибок удаления в последнем запуске: 1" in result["issues"]
    assert "Ожидают автоудаления: 2" in result["issues"]


def test_get_campaign_control_center_handles_exception():
    class _FailRepo(_FakeRepo):
        def get_rule(self, rule_id):
            raise RuntimeError("boom")

    runtime = RepostCampaignRuntimeService(repo=_FailRepo(), renderer=_FakeRenderer(None))
    result = runtime.get_campaign_control_center(rule_id=9)
    assert result["ok"] is False
    assert "Не удалось загрузить центр управления кампанией" in result["error_text"]


class _FakeChecker:
    def __init__(self, result):
        self.result = result

    async def check_target(self, **kwargs):
        return self.result


def test_check_one_success_saves_result():
    repo=_FakeRepo(rule=SimpleNamespace(mode="repost")); repo._targets=[{"id":1,"target_id":"-1001","title":"A"}]
    checker=_FakeChecker(SimpleNamespace(ok=True,target_id="-1001",title="A",error_text=None,can_view=True,can_publish=True,can_delete=True))
    rt=RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), target_checker=checker)
    result=asyncio.run(rt.check_campaign_target(rule_id=1,target_row_id=1))
    assert result["ok"] is True
    assert repo.update_target_check_calls[0] == (1, "A", None)


def test_check_one_failed_saves_error():
    repo=_FakeRepo(rule=SimpleNamespace(mode="repost")); repo._targets=[{"id":1,"target_id":"-1001","title":"A"}]
    checker=_FakeChecker(SimpleNamespace(ok=False,target_id="-1001",title="A",error_text="err",can_view=True,can_publish=False,can_delete=None))
    rt=RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), target_checker=checker)
    result=asyncio.run(rt.check_campaign_target(rule_id=1,target_row_id=1))
    assert result["ok"] is False
    assert repo.update_target_check_calls[0] == (1, "A", "err")


def test_checker_unavailable():
    rt=RepostCampaignRuntimeService(repo=_FakeRepo(rule=SimpleNamespace(mode="repost")), renderer=_FakeRenderer(None), target_checker=None)
    result=asyncio.run(rt.check_campaign_target(rule_id=1,target_row_id=1))
    assert result["error_text"] == "Сервис проверки прав недоступен"


def test_batch_check_summary():
    repo=_FakeRepo(rule=SimpleNamespace(mode="repost")); repo._targets=[{"id":1,"target_id":"-1"},{"id":2,"target_id":"-2"}]
    class C:
        async def check_target(self, **kwargs):
            tid=kwargs["target_id"]; ok=tid=="-1"
            return SimpleNamespace(ok=ok,target_id=tid,title=tid,error_text=None if ok else "err",can_view=True,can_publish=ok,can_delete=None)
    rt=RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), target_checker=C())
    result=asyncio.run(rt.check_campaign_targets(rule_id=1))
    assert result["checked"] == 2
    assert result["passed"] == 1
    assert result["failed"] == 1


def test_test_send_saves_sent_message_ids():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1001")
    saved_post = {"content_json": {"kind": "album", "media_items": [{"kind": "photo", "file_id": "x"}]}}
    repo = _FakeRepo(rule=rule, saved_post=saved_post)
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="album", chat_id="-1001", message_id=101, message_ids=[101, 102, 103]))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    asyncio.run(runtime.test_send_saved_post_to_main_target(rule_id=1))
    assert repo.mark_campaign_run_message_sent_calls[-1][1]["sent_message_ids"] == [101, 102, 103]


def test_process_due_deletions_uses_sent_message_ids_json():
    repo = _FakeRepo()
    repo._due_delete_rows = [{"id": 1, "target_id": "-100", "sent_message_id": 101, "sent_message_ids_json": "[101,102]", "render_mode": "bot_api"}]
    seen = {}
    async def _del_many(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(ok=True, method="bot_api", error_text=None)
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), deleter=SimpleNamespace(delete_messages=_del_many, delete_message=None))
    asyncio.run(runtime.process_due_deletions())
    assert seen["message_ids"] == [101, 102]
