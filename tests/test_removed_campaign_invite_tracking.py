import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RETIRED_MODULES = {
    "app.campaign_invite_link_events_service",
    "app.campaign_invite_link_update_handlers",
    "app.campaign_invite_links_service",
}
RETIRED_TABLES = (
    "campaign_invite_link_events",
    "campaign_invite_links",
    "campaign_invite_link_settings",
)


def test_retired_tracking_is_not_registered_or_recreated():
    for module in RETIRED_MODULES:
        assert not (ROOT / (module.replace(".", "/") + ".py")).exists()

    bot_tree = ast.parse((ROOT / "bot.py").read_text(encoding="utf-8"))
    for node in ast.walk(bot_tree):
        if isinstance(node, ast.ImportFrom):
            assert node.module not in RETIRED_MODULES
            if node.module:
                assert all(f"{node.module}.{alias.name}" not in RETIRED_MODULES for alias in node.names)
        elif isinstance(node, ast.Import):
            assert not any(alias.name in RETIRED_MODULES for alias in node.names)
        elif isinstance(node, ast.Subscript):
            assert not (
                isinstance(node.value, ast.Name)
                and node.value.id == "dp"
                and isinstance(node.slice, ast.Constant)
                and node.slice.value == "campaign_invite_link_repo"
            )

    for filename in ("repository.py", "postgres_repository.py"):
        tree = ast.parse((ROOT / "app" / filename).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert "campaign_invite_link" not in node.name
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                for table in RETIRED_TABLES:
                    assert not re.search(
                        rf"(?im)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{table}\b",
                        node.value,
                    )


def test_manual_cleanup_is_limited_to_retired_tables():
    source = (ROOT / "deploy/manual/remove_campaign_invite_tracking.sql").read_text(encoding="utf-8")
    statements = [line.strip() for line in source.splitlines() if line.startswith("DROP TABLE")]
    assert statements == [f"DROP TABLE IF EXISTS {table};" for table in RETIRED_TABLES]
    assert "CASCADE" not in source
