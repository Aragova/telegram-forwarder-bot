from pathlib import Path
import ast


ROOT = Path(__file__).resolve().parents[1]


def test_retired_tracking_is_not_registered_or_recreated():
    paths = [ROOT / "bot.py", *(ROOT / "app").rglob("*.py")]
    for path in paths:
        source = path.read_text(encoding="utf-8")
        assert "campaign_invite_link" not in source.lower(), path
        assert "Заявки и вступления" not in source, path
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in {"chat_join_request", "chat_member"}, path


def test_manual_cleanup_is_limited_to_retired_tables():
    source = (ROOT / "deploy/manual/remove_campaign_invite_tracking.sql").read_text(encoding="utf-8")
    statements = [line.strip() for line in source.splitlines() if line.startswith("DROP TABLE")]
    assert statements == [
        "DROP TABLE IF EXISTS campaign_invite_link_events;",
        "DROP TABLE IF EXISTS campaign_invite_links;",
        "DROP TABLE IF EXISTS campaign_invite_link_settings;",
    ]
    assert "CASCADE" not in source
