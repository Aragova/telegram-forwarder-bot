import asyncio
from types import SimpleNamespace

from app.repost_campaign_runtime_service import RepostCampaignRuntimeService, build_telegram_message_url
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
        self.update_target_check_result = True
        self.set_target_active_result = True

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
        return self.set_target_active_result

    def remove_rule_repost_campaign_target(self, target_row_id):
        self.remove_target_calls.append(target_row_id)
        return True

    def update_rule_repost_campaign_target_check_result(self, row_id, *, title=None, last_check_error=None):
        self.update_target_check_calls.append((row_id, title, last_check_error))
        return self.update_target_check_result


class _FakeRenderer:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self.result, list):
            return self.result[len(self.calls) - 1]
        return self.result


class _FakeDeleter:
    def __init__(self, *, result=None):
        self.delete_messages_calls = []
        self.delete_message_calls = []
        self.result = result if result is not None else SimpleNamespace(ok=True, error_text=None, to_dict=lambda: {"ok": True})

    async def delete_messages(self, **kwargs):
        self.delete_messages_calls.append(kwargs)
        return self.result

    async def delete_message(self, **kwargs):
        self.delete_message_calls.append(kwargs)
        return self.result


class _FakeTelethonClient:
    def __init__(self, views_map=None, fail_ids=None, fail_entities=None):
        self.views_map = views_map or {}
        self.fail_ids = set(fail_ids or [])
        self.fail_entities = set(fail_entities or [])
        self.get_messages_calls = []
        self.get_entity_calls = []

    async def get_messages(self, *, entity, ids):
        self.get_messages_calls.append({"entity": entity, "ids": ids})
        if entity in self.fail_entities:
            raise ValueError(f'Cannot find any entity corresponding to "{entity}"')
        if ids in self.fail_ids:
            raise RuntimeError("boom")
        if ids not in self.views_map:
            return None
        value = self.views_map[ids]
        if hasattr(value, "views") or hasattr(value, "id"):
            return value
        if isinstance(value, dict):
            return SimpleNamespace(**value)
        return SimpleNamespace(id=ids, views=value)

    async def get_entity(self, entity):
        self.get_entity_calls.append(entity)
        if entity in self.fail_entities:
            raise ValueError(f'Cannot find any entity corresponding to "{entity}"')
        return entity


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


def test_preview_saved_post_in_main_target_uses_rule_target():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1002451047809")
    saved_post = {"id": 55, "content_json": {"kind": "photo"}}
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="photo", chat_id="-1002451047809", message_id=1025))
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=rule, saved_post=saved_post), renderer=renderer)
    result = asyncio.run(runtime.preview_saved_post_in_main_target(rule_id=3, admin_chat_id=123))
    assert renderer.calls[0]["chat_id"] == "-1002451047809"
    assert result.ok is True
    assert result.extra["target_id"] == "-1002451047809"


def test_preview_saved_post_in_main_target_album_ids_and_url():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, target_id="-1002451047809")
    saved_post = {"id": 55, "content_json": {"kind": "album"}}
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="telethon_builder", kind="album", chat_id="-1002451047809", message_id=1025, message_ids=[1025, 1026, 1027]))
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=rule, saved_post=saved_post), renderer=renderer)
    result = asyncio.run(runtime.preview_saved_post_in_main_target(rule_id=3, admin_chat_id=123))
    assert result.extra["message_ids"] == [1025, 1026, 1027]
    assert result.extra["preview_url"] == "https://t.me/c/2451047809/1025"


def test_build_telegram_message_url_private_channel():
    assert build_telegram_message_url(target_id="-1002451047809", message_id=1025) == "https://t.me/c/2451047809/1025"


def test_delete_preview_uses_delete_messages_for_album():
    deleter = _FakeDeleter()
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=SimpleNamespace(mode="repost"), saved_post={}), renderer=_FakeRenderer(None), deleter=deleter)
    result = asyncio.run(runtime.delete_preview_messages(target_id="-1001", message_ids=[1, 2, 3], render_mode="telethon_builder"))
    assert result.ok is True
    assert len(deleter.delete_messages_calls) == 1
    assert deleter.delete_messages_calls[0]["message_ids"] == [1, 2, 3]


def test_delete_preview_single_message():
    deleter = _FakeDeleter()
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=SimpleNamespace(mode="repost"), saved_post={}), renderer=_FakeRenderer(None), deleter=deleter)
    result = asyncio.run(runtime.delete_preview_messages(target_id="-1001", message_id=10, render_mode="bot_api"))
    assert result.ok is True
    assert len(deleter.delete_message_calls) == 1
    assert deleter.delete_message_calls[0]["message_id"] == 10


def test_delete_preview_returns_fail_when_deleter_result_not_ok():
    deleter = _FakeDeleter(result=SimpleNamespace(ok=False, error_text="no rights", to_dict=lambda: {"ok": False, "error_text": "no rights"}))
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=SimpleNamespace(mode="repost"), saved_post={}), renderer=_FakeRenderer(None), deleter=deleter)
    result = asyncio.run(runtime.delete_preview_messages(target_id="-1001", message_id=10, render_mode="bot_api"))
    assert result.ok is False
    assert result.error_text == "no rights"


def test_build_views_report_all_available():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3, "saved_post_id": 22, "show_seconds": 3600}
    repo._messages = [
        {"send_status": "sent", "target_id": "-1001", "target_title": "A", "sent_message_id": 100, "target_kind": "main"},
        {"send_status": "sent", "target_id": "-1002", "target_title": "B", "sent_message_id": 200, "target_kind": "extra"},
    ]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=_FakeTelethonClient({100: 120, 200: 80}))
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["ok"] is True and report["status"] == "ready"
    assert report["views_total"] == 200 and report["views_available"] == 2 and report["views_unavailable"] == 0


def test_build_views_report_normalizes_negative_channel_id():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1002451047809", "target_title": "A", "sent_message_id": 1062}]
    telethon = _FakeTelethonClient({1062: 77})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=telethon)
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["status"] == "ready"
    assert report["views_total"] > 0
    assert report["views_available"] == 1 and report["views_unavailable"] == 0
    assert telethon.get_messages_calls[0]["entity"] == -1002451047809


def test_build_views_report_album_counts_first_message_only():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1001", "target_title": "A", "sent_message_ids": [100, 101, 102, 103, 104]}]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=_FakeTelethonClient({100: 500}))
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["views_total"] == 500
    assert report["items"][0]["is_album"] is True and report["items"][0]["album_items"] == 5


def test_build_views_report_partial_unavailable():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [
        {"send_status": "sent", "target_id": "-1001", "target_title": "A", "sent_message_id": 100},
        {"send_status": "sent", "target_id": "-1002", "target_title": "B", "sent_message_id": 200},
    ]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=_FakeTelethonClient({100: 10}, fail_ids={200}))
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["status"] == "partial" and report["views_available"] == 1 and report["views_unavailable"] == 1
    assert len(report["problem_items"]) == 1


def test_build_views_report_after_deleted_unavailable():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1001", "target_title": "A", "sent_message_id": 100, "delete_status": "deleted"}]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=_FakeTelethonClient({}))
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["items"][0]["views_status"] == "unavailable"
    assert "Публикация уже удалена" in report["items"][0]["error_text"]


def test_build_views_report_entity_failure_uses_human_error():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1002451047809", "target_title": "A", "sent_message_id": 1062}]
    telethon = _FakeTelethonClient({1062: 77}, fail_entities={-1002451047809})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=telethon)
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["status"] in {"unavailable", "partial"}
    assert "Cannot find any entity" not in report["items"][0]["error_text"]
    assert "Telegram не вернул просмотры" in report["items"][0]["error_text"]


def test_build_views_report_no_telethon():
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=SimpleNamespace(mode="repost")), renderer=_FakeRenderer(None), telethon_client=None)
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["ok"] is False
    assert "Сервис сбора просмотров" in report["error_text"]


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
    assert result["is_active"] is False
    assert repo.set_target_active_calls[-1] == (1, False)


def test_set_campaign_target_active_repo_returns_false():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._targets = [{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}]
    repo.set_target_active_result = False
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = runtime.set_campaign_target_active(rule_id=3, target_row_id=1, is_active=False)
    assert result["ok"] is False
    assert result["error_text"]


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
    assert result.error_text == "Кампания не готова к запуску"
    assert len(renderer.calls) == 0


def test_launch_campaign_album_not_marked_sent_without_message_ids():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=43200, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "album"}})
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="telethon_source", kind="album", chat_id="-1001", message_id=1, message_ids=[]))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert len(repo.mark_campaign_run_message_sent_calls) == 0
    assert len(repo.mark_campaign_run_message_failed_calls) == 1
    assert result.ok is False
    assert result.extra["targets_failed"] == 1


def test_launch_campaign_album_marked_sent_with_verified_ids():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=43200, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind": "album"}})
    renderer = _FakeRenderer(SavedPostRenderResult(ok=True, method="telethon_source", kind="album", chat_id="-1001", message_id=200, message_ids=[200, 201]))
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert len(repo.mark_campaign_run_message_sent_calls) == 1
    sent_kwargs = repo.mark_campaign_run_message_sent_calls[0][1]
    assert sent_kwargs["sent_message_ids"] == [200, 201]
    assert result.ok is True

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
    assert result["saved"] is True
    assert repo.update_target_check_calls[0] == (1, "A", None)


def test_check_one_failed_saves_error():
    repo=_FakeRepo(rule=SimpleNamespace(mode="repost")); repo._targets=[{"id":1,"target_id":"-1001","title":"A"}]
    checker=_FakeChecker(SimpleNamespace(ok=False,target_id="-1001",title="A",error_text="err",can_view=True,can_publish=False,can_delete=None))
    rt=RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), target_checker=checker)
    result=asyncio.run(rt.check_campaign_target(rule_id=1,target_row_id=1))
    assert result["ok"] is False
    assert result["check_ok"] is False
    assert repo.update_target_check_calls[0] == (1, "A", "err")


def test_check_one_ok_but_save_failed():
    repo=_FakeRepo(rule=SimpleNamespace(mode="repost")); repo._targets=[{"id":1,"target_id":"-1001","title":"A"}]
    repo.update_target_check_result = False
    checker=_FakeChecker(SimpleNamespace(ok=True,target_id="-1001",title="A",error_text=None,can_view=True,can_publish=True,can_delete=True))
    rt=RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), target_checker=checker)
    result=asyncio.run(rt.check_campaign_target(rule_id=1,target_row_id=1))
    assert result["ok"] is False
    assert result["check_ok"] is True
    assert result["saved"] is False
    assert "не удалось сохранить результат" in result["error_text"]


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


def test_build_launch_readiness_ready():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=300, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"id":55})
    repo._targets = [{"id":1,"target_id":"-1002","is_active":True,"last_check_error":None},{"id":2,"target_id":"-1003","is_active":False,"last_check_error":None}]
    readiness = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None)).build_campaign_launch_readiness(rule_id=1)
    assert readiness["can_launch"] is True
    assert readiness["will_send_total"] == 2
    assert readiness["extra_ready"] == 1
    assert readiness["extra_paused"] == 1
    assert readiness["extra_problem"] == 0

def test_build_launch_readiness_blocks_active_problem():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=300, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"id":55})
    repo._targets = [{"id":1,"target_id":"-1002","is_active":True,"last_check_error":"err"}]
    readiness = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None)).build_campaign_launch_readiness(rule_id=1)
    assert readiness["can_launch"] is False
    assert readiness["extra_active_problem"] == 1
    assert any("требуют настройки" in x for x in readiness["block_reasons"])

def test_launch_campaign_now_does_not_create_run_when_blocked():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=300, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind":"text"}})
    repo._targets = [{"id":1,"target_id":"-1002","is_active":True,"last_check_error":"err"}]
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1001", message_id=1)))
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.ok is False
    assert result.extra.get("launch_readiness")
    assert not repo.create_campaign_run_calls

def test_launch_campaign_now_uses_only_ready_targets():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=300, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind":"text"}})
    repo._targets = [{"id":1,"target_id":"-1002","is_active":True,"last_check_error":None},{"id":2,"target_id":"-1003","is_active":False,"last_check_error":None},{"id":3,"target_id":"-1004","is_active":False,"last_check_error":"err"}]
    renderer = _FakeRenderer([SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1001", message_id=1), SavedPostRenderResult(ok=True, method="bot_api", kind="text", chat_id="-1002", message_id=2)])
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=renderer)
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.ok is True
    assert repo.create_campaign_run_calls[0]["targets_total"] == 2
    assert len(repo.create_campaign_run_message_calls) == 2

def test_launch_campaign_missing_show_seconds_blocked():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=55, repost_campaign_show_seconds=0, target_id="-1001")
    repo = _FakeRepo(rule=rule, saved_post={"content_json": {"kind":"text"}})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None))
    result = asyncio.run(runtime.launch_campaign_now(rule_id=1))
    assert result.ok is False
    assert not repo.create_campaign_run_calls


def test_build_views_report_counts_only_matching_message_id():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1002741117827", "target_title": "A", "sent_message_id": 2484}]
    telethon = _FakeTelethonClient({2484: {"id": 9999, "views": 9023, "peer_id": SimpleNamespace(channel_id=2741117827)}})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=telethon)
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["views_total"] == 0
    assert report["views_available"] == 0
    assert report["views_unavailable"] == 1
    assert report["items"][0]["views_status"] == "failed"
    assert "не подтвердил нужное сообщение" in report["items"][0]["error_text"]


def test_build_views_report_counts_only_matching_peer():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1002741117827", "target_title": "A", "sent_message_id": 2484}]
    telethon = _FakeTelethonClient({2484: {"id": 2484, "views": 9023, "peer_id": SimpleNamespace(channel_id=2535740454)}})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=telethon)
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["views_total"] == 0
    assert report["items"][0]["views_status"] == "failed"
    assert "другого канала" in report["items"][0]["error_text"]


def test_build_views_report_counts_matching_peer_and_id():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1002741117827", "target_title": "A", "sent_message_id": 2484}]
    telethon = _FakeTelethonClient({2484: {"id": 2484, "views": 129, "peer_id": SimpleNamespace(channel_id=2741117827)}})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=telethon)
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["views_total"] == 129
    assert report["views_available"] == 1
    assert report["items"][0]["views_status"] == "ok"


def test_normalize_telegram_channel_id_for_compare():
    runtime = RepostCampaignRuntimeService(repo=_FakeRepo(rule=SimpleNamespace(mode="repost")), renderer=_FakeRenderer(None))
    assert runtime._normalize_telegram_channel_id_for_compare("-1002741117827") == "2741117827"
    assert runtime._normalize_telegram_channel_id_for_compare(-1002741117827) == "2741117827"
    assert runtime._normalize_telegram_channel_id_for_compare("2741117827") == "2741117827"


def test_album_first_message_peer_validated():
    repo = _FakeRepo(rule=SimpleNamespace(mode="repost"))
    repo._run = {"id": 8, "rule_id": 3}
    repo._messages = [{"send_status": "sent", "target_id": "-1002741117827", "target_title": "A", "sent_message_ids": [2484, 2485, 2486]}]
    telethon = _FakeTelethonClient({2484: {"id": 2484, "views": 129, "peer_id": SimpleNamespace(channel_id=2741117827)}})
    runtime = RepostCampaignRuntimeService(repo=repo, renderer=_FakeRenderer(None), telethon_client=telethon)
    report = asyncio.run(runtime.build_campaign_views_report(rule_id=3, run_id=8))
    assert report["views_total"] == 129
    assert report["views_available"] == 1
    assert report["items"][0]["is_album"] is True
