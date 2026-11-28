"""A runtime start is not a delivery retry authorization."""
import ast
from pathlib import Path

from app.postgres_repository import PostgresRepository


def test_all_runtime_roles_share_nondestructive_db_initialization():
    source = Path("bot.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    init = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
                and n.name == "_init_db_runtime")
    body = ast.get_source_segment(source, init)
    assert "db.integrity_check" in body
    assert "db.reset_stuck_processing" not in body


def test_legacy_global_reset_never_touches_database():
    class NoDatabase:
        def connect(self):
            raise AssertionError("legacy reset must not open a transaction")

    assert PostgresRepository.reset_stuck_processing(NoDatabase()) == 0
