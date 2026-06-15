from pathlib import Path

from app.repost_campaign_top_time import normalize_repost_campaign_top_time_settings
from app.repository import RepositoryProtocol
from app.postgres_repository import PostgresRepository


def test_top_time_snapshot_normalization():
    assert normalize_repost_campaign_top_time_settings(enabled=False, seconds=7200) == {"enabled": False, "seconds": 0}
    assert normalize_repost_campaign_top_time_settings(enabled=True, seconds=7200) == {"enabled": True, "seconds": 7200}
    assert normalize_repost_campaign_top_time_settings(enabled=True, seconds=123) == {"enabled": True, "seconds": 7200}
    assert normalize_repost_campaign_top_time_settings(enabled=True, seconds=None) == {"enabled": True, "seconds": 7200}


def test_repository_protocol_and_postgres_accept_top_time_snapshot_args():
    source = Path("app/repository.py").read_text(encoding="utf-8")
    postgres_source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert hasattr(RepositoryProtocol, "create_campaign_run")
    assert hasattr(RepositoryProtocol, "create_campaign_scheduled_launch")
    assert hasattr(PostgresRepository, "create_campaign_run")
    assert hasattr(PostgresRepository, "create_campaign_scheduled_launch")
    assert "top_time_enabled_snapshot: bool = False" in source
    assert "top_time_seconds_snapshot: int = 0" in source
    assert "top_time_enabled_snapshot: bool = False" in postgres_source
    assert "top_time_seconds_snapshot: int = 0" in postgres_source


def test_campaign_run_schema_and_insert_store_snapshot_columns():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "top_time_enabled_snapshot BOOLEAN NOT NULL DEFAULT FALSE" in source
    assert "top_time_seconds_snapshot INTEGER NOT NULL DEFAULT 0" in source
    assert "ALTER TABLE campaign_runs ADD COLUMN IF NOT EXISTS top_time_enabled_snapshot BOOLEAN NOT NULL DEFAULT FALSE" in source
    assert "ALTER TABLE campaign_runs ADD COLUMN IF NOT EXISTS top_time_seconds_snapshot INTEGER NOT NULL DEFAULT 0" in source
    assert "INSERT INTO campaign_runs" in source
    assert "top_time_enabled_snapshot, top_time_seconds_snapshot" in source
    assert "normalize_repost_campaign_top_time_settings" in source


def test_scheduled_launch_schema_insert_and_claim_return_snapshot_columns():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS top_time_enabled_snapshot BOOLEAN NOT NULL DEFAULT FALSE" in source
    assert "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS top_time_seconds_snapshot INTEGER NOT NULL DEFAULT 0" in source
    assert "INSERT INTO campaign_scheduled_launches" in source
    assert "preview_json,top_time_enabled_snapshot,top_time_seconds_snapshot" in source
    assert "RETURNING s.*" in source
    assert "SELECT * FROM campaign_scheduled_launches" in source
