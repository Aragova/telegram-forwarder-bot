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

        if normalized.startswith("select id from intros where rule_id") and "file_path = %s" not in normalized:
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


        if normalized.startswith("select id, tenant_id, video_intro_horizontal_id, video_intro_vertical_id"):
            self._rows = [
                dict(row)
                for row in sorted(self.db.routing_rows, key=lambda row: row["id"])
                if row.get("video_intro_horizontal_id") is not None
                or row.get("video_intro_vertical_id") is not None
            ]
            return None

        if normalized.startswith("select id from intros") and "file_path = %s" in normalized:
            rule_id, file_path, display_name = params
            found = next(
                (
                    row
                    for row in self.db.rows
                    if row["rule_id"] == rule_id
                    and row["file_path"] == file_path
                    and row["display_name"] == display_name
                    and self.db.is_active(row)
                ),
                None,
            )
            self._row = {"id": found["id"]} if found else None
            return None

        if normalized.startswith("update routing set video_intro_horizontal_id"):
            intro_id, rule_id = params
            row = self.db.get_routing(rule_id)
            if row:
                row["video_intro_horizontal_id"] = intro_id
                self.rowcount = 1
            return None

        if normalized.startswith("update routing set video_intro_vertical_id"):
            intro_id, rule_id = params
            row = self.db.get_routing(rule_id)
            if row:
                row["video_intro_vertical_id"] = intro_id
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
        self.routing_rows: list[dict[str, Any]] = []
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

    def add_routing(
        self,
        rule_id: int,
        *,
        horizontal_id: int | None = None,
        vertical_id: int | None = None,
        tenant_id: int = 1,
    ) -> None:
        self.routing_rows.append({
            "id": rule_id,
            "tenant_id": tenant_id,
            "video_intro_horizontal_id": horizontal_id,
            "video_intro_vertical_id": vertical_id,
        })

    def get_routing(self, rule_id: int) -> dict[str, Any] | None:
        return next((row for row in self.routing_rows if row["id"] == rule_id), None)

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
    repository._test_intro_db = db  # type: ignore[attr-defined]
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


def _test_db(repo) -> _IntroDb:
    return repo._test_intro_db  # type: ignore[attr-defined]


def test_migrate_legacy_rule_intro_assignments_horizontal_global_intro(repo):
    global_intro_id = repo.add_intro("old_intro", "old_intro.mp4", "/tmp/old_intro.mp4", 8)
    db = _test_db(repo)
    db.add_routing(5, horizontal_id=global_intro_id)

    stats = repo.migrate_legacy_rule_intro_assignments()

    routing = db.get_routing(5)
    assert routing is not None
    new_intro_id = routing["video_intro_horizontal_id"]
    assert new_intro_id != global_intro_id
    assert db.get(global_intro_id)["rule_id"] is None
    assert db.get(new_intro_id)["rule_id"] == 5
    assert stats["assignments_migrated"] == 1
    assert stats["intro_copies_created"] == 1


def test_migrate_legacy_rule_intro_assignments_reuses_one_copy_for_both_fields(repo):
    global_intro_id = repo.add_intro("old_intro", "old_intro.mp4", "/tmp/old_intro.mp4", 8)
    db = _test_db(repo)
    db.add_routing(5, horizontal_id=global_intro_id, vertical_id=global_intro_id)

    stats = repo.migrate_legacy_rule_intro_assignments()

    routing = db.get_routing(5)
    assert routing["video_intro_horizontal_id"] == routing["video_intro_vertical_id"]
    assert routing["video_intro_horizontal_id"] != global_intro_id
    assert stats["assignments_migrated"] == 2
    assert stats["intro_copies_created"] == 1


def test_migrate_legacy_rule_intro_assignments_creates_copy_per_rule(repo):
    global_intro_id = repo.add_intro("shared", "shared.mp4", "/tmp/shared.mp4", 8)
    db = _test_db(repo)
    db.add_routing(1, horizontal_id=global_intro_id)
    db.add_routing(2, horizontal_id=global_intro_id)

    stats = repo.migrate_legacy_rule_intro_assignments()

    first = db.get_routing(1)["video_intro_horizontal_id"]
    second = db.get_routing(2)["video_intro_horizontal_id"]
    assert first != second
    assert db.get(first)["rule_id"] == 1
    assert db.get(second)["rule_id"] == 2
    assert stats["assignments_migrated"] == 2
    assert stats["intro_copies_created"] == 2


def test_migrate_legacy_rule_intro_assignments_is_idempotent(repo):
    global_intro_id = repo.add_intro("old_intro", "old_intro.mp4", "/tmp/old_intro.mp4", 8)
    db = _test_db(repo)
    db.add_routing(5, horizontal_id=global_intro_id)

    first_stats = repo.migrate_legacy_rule_intro_assignments()
    rows_after_first_run = len(db.rows)
    second_stats = repo.migrate_legacy_rule_intro_assignments()

    assert first_stats["assignments_migrated"] == 1
    assert len(db.rows) == rows_after_first_run
    assert second_stats["assignments_migrated"] == 0
    assert second_stats["intro_copies_created"] == 0


def test_migrate_legacy_rule_intro_assignments_copies_other_rule_intro(repo):
    source_intro_id = repo.add_rule_intro(1, "rule_one", "rule_one.mp4", "/tmp/rule_one.mp4", 8)
    db = _test_db(repo)
    db.add_routing(2, horizontal_id=source_intro_id)

    stats = repo.migrate_legacy_rule_intro_assignments()

    new_intro_id = db.get_routing(2)["video_intro_horizontal_id"]
    assert new_intro_id != source_intro_id
    assert db.get(new_intro_id)["rule_id"] == 2
    assert stats["assignments_migrated"] == 1
    assert stats["intro_copies_created"] == 1


def test_migrate_legacy_rule_intro_assignments_skips_missing_intro(repo):
    db = _test_db(repo)
    db.add_routing(5, horizontal_id=999999)

    stats = repo.migrate_legacy_rule_intro_assignments()

    assert db.get_routing(5)["video_intro_horizontal_id"] == 999999
    assert stats["missing_intro_skipped"] == 1
    assert stats["assignments_migrated"] == 0


def test_intro_legacy_migration_method_exists():
    source = Path("app/repository.py").read_text(encoding="utf-8")
    assert "migrate_legacy_rule_intro_assignments" in source

    pg_source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    assert "def migrate_legacy_rule_intro_assignments" in pg_source
    assert "INTRO_LEGACY_ASSIGNMENTS_MIGRATED" in pg_source
