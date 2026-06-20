from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from app.postgres_repository import PostgresRepository


class _Cursor:
    def __init__(self, store: dict[int, dict]):
        self.store = store
        self.rowcount = 0
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        params = params or ()
        if normalized.startswith("SELECT * FROM campaign_invite_link_settings"):
            self._row = self.store.get(int(params[0]))
            self.rowcount = 1 if self._row else 0
            return
        if normalized.startswith("INSERT INTO campaign_invite_link_settings(rule_id) VALUES"):
            rule_id = int(params[0])
            self.store.setdefault(rule_id, _default_row(rule_id))
            self.rowcount = 1
            return
        if normalized.startswith("UPDATE campaign_invite_link_settings SET"):
            rule_id = int(params[-1])
            row = self.store[rule_id]
            columns = [
                "enabled",
                "destination_chat_id",
                "destination_chat_title",
                "link_mode",
                "injection_mode",
                "append_template",
                "per_target_links_enabled",
                "preview_required",
            ]
            for column, value in zip(columns, params[: len(columns)]):
                if value is not None:
                    row[column] = value
            if params[8]:
                row["preview_checked_at"] = None
                row["preview_checked_by"] = None
            row["updated_at"] = datetime.now(timezone.utc)
            self.rowcount = 1
            return
        if normalized.startswith("INSERT INTO campaign_invite_link_settings(rule_id, preview_checked_at, preview_checked_by)"):
            rule_id = int(params[0])
            row = self.store.setdefault(rule_id, _default_row(rule_id))
            row["preview_checked_at"] = params[1] or datetime.now(timezone.utc)
            row["preview_checked_by"] = params[2]
            row["updated_at"] = datetime.now(timezone.utc)
            self.rowcount = 1
            return
        if normalized.startswith("SELECT COUNT(*) AS cnt FROM campaign_invite_link_settings"):
            self._row = {"cnt": 1 if int(params[0]) in self.store else 0}
            self.rowcount = 1
            return
        raise AssertionError(f"Unexpected SQL: {normalized}")

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, store: dict[int, dict]):
        self.store = store

    def cursor(self):
        return _Cursor(self.store)

    def commit(self):
        return None


@contextmanager
def _connect(store):
    yield _Conn(store)


def _default_row(rule_id: int) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "rule_id": rule_id,
        "enabled": False,
        "destination_chat_id": None,
        "destination_chat_title": None,
        "link_mode": "join_request",
        "injection_mode": "placeholder",
        "append_template": "👉 Подписаться: {invite_link}",
        "per_target_links_enabled": True,
        "preview_required": True,
        "preview_checked_at": None,
        "preview_checked_by": None,
        "created_at": now,
        "updated_at": now,
    }


def _repo():
    store: dict[int, dict] = {}
    repo = PostgresRepository()
    repo.connect = lambda: _connect(store)
    return repo, store


def test_default_settings_do_not_create_row():
    repo, store = _repo()

    settings = repo.get_campaign_invite_link_settings(101)

    assert settings["enabled"] is False
    assert settings["destination_chat_id"] is None
    assert settings["destination_chat_title"] is None
    assert settings["link_mode"] == "join_request"
    assert settings["injection_mode"] == "placeholder"
    assert settings["append_template"] == "👉 Подписаться: {invite_link}"
    assert settings["per_target_links_enabled"] is True
    assert settings["preview_required"] is True
    assert settings["preview_checked_at"] is None
    assert settings["preview_checked_by"] is None
    assert store == {}


def test_upsert_creates_row():
    repo, _store = _repo()

    assert repo.set_campaign_invite_link_settings(102, enabled=True) is True

    assert repo.get_campaign_invite_link_settings(102)["enabled"] is True


def test_update_existing_row():
    repo, _store = _repo()

    repo.set_campaign_invite_link_settings(103, enabled=True)
    repo.set_campaign_invite_link_settings(103, enabled=False)

    assert repo.get_campaign_invite_link_settings(103)["enabled"] is False


def test_save_modes():
    repo, _store = _repo()

    assert repo.set_campaign_invite_link_settings(
        104,
        link_mode="direct_join",
        injection_mode="append_footer",
        append_template="🔥 Войти: {invite_link}",
        per_target_links_enabled=False,
        preview_required=False,
    ) is True

    settings = repo.get_campaign_invite_link_settings(104)
    assert settings["link_mode"] == "direct_join"
    assert settings["injection_mode"] == "append_footer"
    assert settings["append_template"] == "🔥 Войти: {invite_link}"
    assert settings["per_target_links_enabled"] is False
    assert settings["preview_required"] is False


def test_preview_checked():
    repo, _store = _repo()

    assert repo.mark_campaign_invite_link_preview_checked(105, actor_id=123) is True

    settings = repo.get_campaign_invite_link_settings(105)
    assert settings["preview_checked_at"] is not None
    assert settings["preview_checked_by"] == 123


def test_reset_preview_after_critical_setting_change():
    repo, _store = _repo()

    repo.mark_campaign_invite_link_preview_checked(106, actor_id=123)
    repo.set_campaign_invite_link_settings(106, injection_mode="disabled")

    settings = repo.get_campaign_invite_link_settings(106)
    assert settings["preview_checked_at"] is None
    assert settings["preview_checked_by"] is None


def test_do_not_reset_preview_on_enabled_toggle():
    repo, _store = _repo()

    repo.mark_campaign_invite_link_preview_checked(107, actor_id=123)
    before = repo.get_campaign_invite_link_settings(107)["preview_checked_at"]
    repo.set_campaign_invite_link_settings(107, enabled=True)

    settings = repo.get_campaign_invite_link_settings(107)
    assert settings["preview_checked_at"] == before
    assert settings["preview_checked_by"] == 123


def test_no_duplicate_rows():
    repo, store = _repo()

    repo.set_campaign_invite_link_settings(108, enabled=True)
    repo.set_campaign_invite_link_settings(108, enabled=False)
    repo.set_campaign_invite_link_settings(108, link_mode="direct_join")

    assert len([rule_id for rule_id in store if rule_id == 108]) == 1


def test_invalid_link_mode_rejected():
    repo, _store = _repo()

    assert repo.set_campaign_invite_link_settings(109, link_mode="bad_mode") is False

    assert repo.get_campaign_invite_link_settings(109)["link_mode"] == "join_request"


def test_invalid_injection_mode_rejected():
    repo, _store = _repo()

    assert repo.set_campaign_invite_link_settings(110, injection_mode="bad_mode") is False

    assert repo.get_campaign_invite_link_settings(110)["injection_mode"] == "placeholder"
