from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from app.postgres_repository import PostgresRepository


class _IntroCursor:
    def __init__(self, db: "_IntroDb") -> None:
        self.db = db
        self._row: dict[str, Any] | None = None
        self._rows: list[dict[str, Any]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows

    def execute(self, sql: str, params: tuple[Any, ...] | None = None):
        normalized = " ".join(sql.lower().split())
        params = params or ()
        self._row = None
        self._rows = []
        self.rowcount = 0

        if normalized.startswith("insert into intros(display_name"):
            display_name, file_name, file_path, duration, created_at = params
            exists = self.db.find_active_by_name(None, display_name)
            if exists is not None:
                return None
            self._row = {"id": self.db.insert(
                rule_id=None,
                tenant_id=1,
                created_by=None,
                display_name=display_name,
                file_name=file_name,
                file_path=file_path,
                duration=duration,
                media_kind=None,
                created_at=created_at,
            )}
            self.rowcount = 1
            return None

        if normalized.startswith("insert into intros("):
            (
                rule_id,
                tenant_id,
                created_by,
                display_name,
                file_name,
                file_path,
                duration,
                media_kind,
                created_at,
            ) = params
            if self.db.find_active_by_name(rule_id, display_name) is not None:
                raise Exception("duplicate intro display_name in rule")
            self._row = {"id": self.db.insert(
                rule_id=rule_id,
                tenant_id=tenant_id,
                created_by=created_by,
                display_name=display_name,
                file_name=file_name,
                file_path=file_path,
                duration=duration,
                media_kind=media_kind,
                created_at=created_at,
            )}
            self.rowcount = 1
            return None

        if normalized.startswith("select id from intros where rule_id"):
            rule_id, display_name = params
            found = self.db.find_active_by_name(rule_id, display_name)
            self._row = {"id": found["id"]} if found else None
            return None

        if normalized.startswith("select id, display_name") and "where id = %s and rule_id = %s" in normalized:
            intro_id, rule_id = params
            include_deleted = "status = 'active'" not in normalized
            row = self.db.get(intro_id)
            if row and row["rule_id"] == rule_id and (include_deleted or self.db.is_active(row)):
                self._row = dict(row)
            return None

        if normalized.startswith("select id, display_name") and "where id = %s" in normalized:
            intro_id = params[0]
            row = self.db.get(intro_id)
            if row and self.db.is_active(row):
                self._row = dict(row)
            return None

        if normalized.startswith("select id, display_name") and "where rule_id = %s" in normalized:
            rule_id = params[0]
            include_deleted = "status = 'active'" not in normalized
            rows = [dict(row) for row in self.db.rows if row["rule_id"] == rule_id]
            if not include_deleted:
                rows = [row for row in rows if self.db.is_active(row)]
            self._rows = sorted(rows, key=lambda row: (row["created_at"], row["id"]), reverse=True)
            return None

        if normalized.startswith("select id, display_name") and "from intros" in normalized:
            self._rows = [dict(row) for row in self.db.rows if self.db.is_active(row)]
            self._rows = sorted(self._rows, key=lambda row: (row["created_at"], row["id"]), reverse=True)
            return None

        if normalized.startswith("update intros set status = 'deleted'"):
            intro_id, rule_id = params
            row = self.db.get(intro_id)
            if row and row["rule_id"] == rule_id and self.db.is_active(row):
                row["status"] = "deleted"
                row["deleted_at"] = "2026-06-06T00:00:00+00:00"
                row["updated_at"] = "2026-06-06T00:00:00+00:00"
                self._row = {"id": intro_id}
                self.rowcount = 1
            return None

        if normalized.startswith("delete from intros where id = %s"):
            intro_id = params[0]
            before = len(self.db.rows)
            self.db.rows = [row for row in self.db.rows if row["id"] != intro_id]
            self.rowcount = before - len(self.db.rows)
            return None

        raise AssertionError(f"Unexpected SQL: {sql}")


class _IntroConn:
    def __init__(self, db: "_IntroDb") -> None:
        self.db = db

    def cursor(self):
        return _IntroCursor(self.db)

    def commit(self):
        return None


class _IntroDb:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.next_id = 1

    def insert(self, **values: Any) -> int:
        intro_id = self.next_id
        self.next_id += 1
        row = {
            "id": intro_id,
            "rule_id": values["rule_id"],
            "tenant_id": values["tenant_id"],
            "created_by": values["created_by"],
            "display_name": values["display_name"],
            "file_name": values["file_name"],
            "file_path": values["file_path"],
            "duration": values["duration"],
            "media_kind": values["media_kind"],
            "status": "active",
            "created_at": f"2026-06-06T00:00:{intro_id:02d}+00:00",
            "deleted_at": None,
            "updated_at": "2026-06-06T00:00:00+00:00",
        }
        self.rows.append(row)
        return intro_id

    def get(self, intro_id: int) -> dict[str, Any] | None:
        return next((row for row in self.rows if row["id"] == intro_id), None)

    def is_active(self, row: dict[str, Any]) -> bool:
        return row["status"] == "active" and row["deleted_at"] is None

    def find_active_by_name(self, rule_id: int | None, display_name: str) -> dict[str, Any] | None:
        return next(
            (
                row
                for row in self.rows
                if row["rule_id"] == rule_id
                and row["display_name"].lower() == display_name.strip().lower()
                and self.is_active(row)
            ),
            None,
        )


@pytest.fixture()
def repo():
    db = _IntroDb()
    repository = PostgresRepository()

    @contextmanager
    def _connect():
        yield _IntroConn(db)

    repository.connect = _connect  # type: ignore[method-assign]
    return repository


def test_can_create_intro_for_specific_rule(repo):
    intro_id = repo.add_rule_intro(
        rule_id=101,
        display_name="main_intro",
        file_name="main_intro.mp4",
        file_path="/tmp/main_intro.mp4",
        duration=8,
        created_by=123,
        media_kind="video",
    )

    items = repo.list_rule_intros(101)

    assert len(items) == 1
    assert items[0].id == intro_id
    assert items[0].rule_id == 101
    assert items[0].display_name == "main_intro"
    assert items[0].status == "active"


def test_rule_intro_isolation(repo):
    intro_a_id = repo.add_rule_intro(101, "intro_a", "intro_a.mp4", "/tmp/intro_a.mp4")
    intro_b_id = repo.add_rule_intro(202, "intro_b", "intro_b.mp4", "/tmp/intro_b.mp4")

    assert [item.display_name for item in repo.list_rule_intros(101)] == ["intro_a"]
    assert [item.display_name for item in repo.list_rule_intros(202)] == ["intro_b"]
    assert repo.get_rule_intro(101, intro_b_id) is None
    assert repo.get_rule_intro(202, intro_a_id) is None


def test_same_display_name_is_allowed_in_different_rules(repo):
    first_id = repo.add_rule_intro(101, "same_name", "same_name_101.mp4", "/tmp/101.mp4")
    second_id = repo.add_rule_intro(202, "same_name", "same_name_202.mp4", "/tmp/202.mp4")

    assert first_id != second_id
    assert repo.list_rule_intros(101)[0].id == first_id
    assert repo.list_rule_intros(202)[0].id == second_id


def test_same_display_name_is_forbidden_inside_one_rule(repo):
    repo.add_rule_intro(101, "same_name", "same_name.mp4", "/tmp/one.mp4")

    with pytest.raises(Exception):
        repo.add_rule_intro(101, "same_name", "same_name_2.mp4", "/tmp/two.mp4")


def test_soft_delete_hides_rule_intro(repo):
    intro_id = repo.add_rule_intro(101, "intro", "intro.mp4", "/tmp/intro.mp4")

    ok = repo.soft_delete_rule_intro(101, intro_id)
    items = repo.list_rule_intros(101)
    deleted_items = repo.list_rule_intros(101, include_deleted=True)

    assert ok is True
    assert items == []
    assert len(deleted_items) == 1
    assert deleted_items[0].status == "deleted"


def test_cannot_soft_delete_intro_from_another_rule(repo):
    intro_id = repo.add_rule_intro(101, "intro", "intro.mp4", "/tmp/intro.mp4")

    ok = repo.soft_delete_rule_intro(202, intro_id)

    assert ok is False
    assert repo.get_rule_intro(101, intro_id).status == "active"


def test_copy_intro_to_rule_creates_copy_with_new_rule_id(repo):
    global_intro_id = repo.add_intro("template", "template.mp4", "/tmp/template.mp4", 8)

    copy_id = repo.copy_intro_to_rule(rule_id=101, intro_id=global_intro_id)
    copy = repo.get_rule_intro(101, copy_id)
    source = repo.get_intro(global_intro_id)

    assert copy is not None
    assert source is not None
    assert copy.id != global_intro_id
    assert copy.rule_id == 101
    assert copy.file_path == source.file_path
    assert copy.display_name.startswith(source.display_name)


def test_copy_intro_to_rule_adds_suffix_on_display_name_conflict(repo):
    global_intro_id = repo.add_intro("template", "template.mp4", "/tmp/template.mp4", 8)
    repo.add_rule_intro(101, "template", "other.mp4", "/tmp/other.mp4")

    copy_id = repo.copy_intro_to_rule(rule_id=101, intro_id=global_intro_id)
    copy = repo.get_rule_intro(101, copy_id)

    assert copy is not None
    assert copy.display_name == "template_2"


def test_intro_repository_protocol_has_rule_scoped_methods():
    source = Path("app/repository.py").read_text(encoding="utf-8")
    assert "add_rule_intro" in source
    assert "list_rule_intros" in source
    assert "get_rule_intro" in source
    assert "soft_delete_rule_intro" in source
    assert "copy_intro_to_rule" in source


def test_postgres_repository_has_rule_scoped_intro_methods():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    assert "def add_rule_intro" in source
    assert "def list_rule_intros" in source
    assert "def get_rule_intro" in source
    assert "def soft_delete_rule_intro" in source
    assert "def copy_intro_to_rule" in source
