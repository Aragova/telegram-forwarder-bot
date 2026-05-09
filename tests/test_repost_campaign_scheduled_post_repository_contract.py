from app.postgres_repository import PostgresRepository


def _mk_repo() -> PostgresRepository:
    return PostgresRepository()


def test_update_campaign_scheduled_post_saves_fields_and_json_contract():
    repo = _mk_repo()
    saved = {}

    def _fake_update(scheduled_post_id: int, **kwargs):
        saved[scheduled_post_id] = dict(kwargs)
        return True

    repo.update_campaign_scheduled_post = _fake_update  # type: ignore[method-assign]
    ok = repo.update_campaign_scheduled_post(
        10,
        saved_post_id=55,
        show_seconds=120,
        scheduled_at="2026-05-10T10:00:00+00:00",
        preview={"a": 1},
        metadata={"k": "v"},
    )
    assert ok is True
    assert saved[10]["saved_post_id"] == 55
    assert saved[10]["show_seconds"] == 120
    assert saved[10]["scheduled_at"] == "2026-05-10T10:00:00+00:00"
    assert saved[10]["preview"] == {"a": 1}
    assert saved[10]["metadata"] == {"k": "v"}


def test_targets_snapshot_and_unique_rules_contract():
    repo = _mk_repo()
    state = {"targets": {}, "posts": {1: {"targets_total": 0}, 2: {"targets_total": 0}}}

    def _replace(*, scheduled_post_id: int, rule_id: int, targets: list[dict]):
        uniq = {}
        for t in targets:
            key = (str(t["target_id"]), t.get("target_thread_id"))
            uniq[key] = dict(t)
        rows = []
        for i, t in enumerate(uniq.values(), start=1):
            row = dict(t)
            row["id"] = i
            row["scheduled_post_id"] = scheduled_post_id
            row["rule_id"] = rule_id
            rows.append(row)
        state["targets"][scheduled_post_id] = rows
        active = sum(1 for r in rows if r.get("is_active", True))
        state["posts"][scheduled_post_id]["targets_total"] = active
        return active

    repo.replace_campaign_scheduled_post_targets = _replace  # type: ignore[method-assign]

    inserted = repo.replace_campaign_scheduled_post_targets(
        scheduled_post_id=1,
        rule_id=11,
        targets=[
            {"target_id": "-1001", "target_thread_id": None, "is_active": True},
            {"target_id": "-1001", "target_thread_id": None, "is_active": True},
        ],
    )
    assert inserted == 1
    assert len(state["targets"][1]) == 1
    assert state["posts"][1]["targets_total"] == 1

    inserted2 = repo.replace_campaign_scheduled_post_targets(
        scheduled_post_id=2,
        rule_id=11,
        targets=[{"target_id": "-1001", "target_thread_id": None, "is_active": True}],
    )
    assert inserted2 == 1
    assert len(state["targets"][2]) == 1


def test_schedule_and_cancel_status_policy_contract():
    repo = _mk_repo()
    posts = {1: "draft", 2: "ready", 3: "processing", 4: "launched"}

    def _schedule(post_id: int, *, scheduled_by=None):
        if posts.get(post_id) in ("draft", "ready"):
            posts[post_id] = "scheduled"
            return True
        return False

    def _cancel(post_id: int, *, cancelled_by=None, reason=None):
        if posts.get(post_id) in ("draft", "ready", "scheduled"):
            posts[post_id] = "cancelled"
            return True
        return False

    repo.schedule_campaign_scheduled_post = _schedule  # type: ignore[method-assign]
    repo.cancel_campaign_scheduled_post = _cancel  # type: ignore[method-assign]

    assert repo.schedule_campaign_scheduled_post(1, scheduled_by=7) is True
    assert repo.schedule_campaign_scheduled_post(2, scheduled_by=7) is True
    assert repo.cancel_campaign_scheduled_post(1, cancelled_by=7, reason="x") is True
    assert repo.cancel_campaign_scheduled_post(3, cancelled_by=7, reason="x") is False
    assert repo.cancel_campaign_scheduled_post(4, cancelled_by=7, reason="x") is False


def test_update_campaign_scheduled_post_target_check_result_real_update_contract():
    repo = _mk_repo()
    rows = [{"id": 1, "can_publish": None, "can_delete": None, "publish_status": None, "delete_status": None, "publish_error_text": None, "delete_error_text": None, "check_source": None, "checked_at": None}]

    def _update(target_row_id: int, **kwargs):
        for r in rows:
            if r["id"] == target_row_id:
                r.update(kwargs)
                r["checked_at"] = "now"
                return True
        return False

    def _list(_scheduled_post_id: int, *, active_only=True):
        return rows

    repo.update_campaign_scheduled_post_target_check_result = _update  # type: ignore[method-assign]
    repo.list_campaign_scheduled_post_targets = _list  # type: ignore[method-assign]

    ok = repo.update_campaign_scheduled_post_target_check_result(
        1,
        can_publish=True,
        can_delete=False,
        publish_status="confirmed",
        delete_status="denied",
        publish_error_text=None,
        delete_error_text="нет прав удаления",
        check_source="telethon",
    )
    assert ok is True
    out = repo.list_campaign_scheduled_post_targets(123, active_only=False)[0]
    assert out["can_publish"] is True
    assert out["can_delete"] is False
    assert out["publish_status"] == "confirmed"
    assert out["delete_status"] == "denied"
    assert out["publish_error_text"] is None
    assert out["delete_error_text"] == "нет прав удаления"
    assert out["check_source"] == "telethon"
    assert out["checked_at"] is not None


def test_checks_and_events_json_dict_contract():
    repo = _mk_repo()
    checks = [{"details_json": {"ok": True}}]
    events = [{"extra_json": {"state": "x"}}]
    repo.list_campaign_scheduled_post_checks = lambda *_a, **_k: checks  # type: ignore[method-assign]
    repo.list_campaign_scheduled_post_events = lambda *_a, **_k: events  # type: ignore[method-assign]
    assert isinstance(repo.list_campaign_scheduled_post_checks(1)[0]["details_json"], dict)
    assert isinstance(repo.list_campaign_scheduled_post_events(1)[0]["extra_json"], dict)
