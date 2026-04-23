from __future__ import annotations

from pathlib import Path

from app.ops_health_service import build_operational_snapshot
from app.preflight_checks import PreflightError, run_preflight_checks
from app.runtime_roles import normalize_runtime_role


class _FakeSettings:
    def __init__(self, root: Path) -> None:
        self.base_dir = root
        self.media_dir = root / "media"
        self.temp_dir = root / "temp"
        self.intros_dir = root / "intros"


class _OkPgClient:
    def is_configured(self) -> bool:
        return True

    def ping(self) -> bool:
        return True


class _BadPgClient:
    def is_configured(self) -> bool:
        return True

    def ping(self) -> bool:
        return False


class _FakeRepo:
    def __init__(self, payload: dict):
        self.payload = payload

    def get_runtime_heartbeats(self):
        return []

    def get_queue_stats(self):
        return {
            "pending": self.payload.get("pending", 0),
            "processing": self.payload.get("processing", 0),
        }

    def count_recent_errors(self, minutes: int = 5):
        return 0

    def get_job_status_counts(self):
        return self.payload.get("jobs", {})

    def get_video_stage_job_counts(self):
        return {}

    def get_expired_leased_jobs(self, limit: int = 100):
        return []

    def get_stuck_processing_jobs(self, stuck_seconds: int = 600, limit: int = 100):
        return []


def _build_health_payload(system_mode: str, roles: dict[str, str]):
    from app import ops_health_service

    def _fake_health(_repo):
        return {
            "roles": roles,
            "pending": 0,
            "processing": 0,
            "system_mode": system_mode,
            "jobs": {"retry": 0, "failed": 0},
        }

    return ops_health_service, _fake_health


def test_preflight_fails_on_missing_required_env(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("APP_PG_HOST", raising=False)
    monkeypatch.setenv("APP_PG_PORT", "5432")
    monkeypatch.setenv("APP_PG_DB", "test")
    monkeypatch.setenv("APP_PG_USER", "test")

    settings = _FakeSettings(tmp_path)

    try:
        run_preflight_checks("scheduler", settings_obj=settings, pg_client_factory=_OkPgClient)
        assert False, "Ожидали PreflightError"
    except PreflightError as exc:
        assert "APP_PG_HOST" in str(exc)


def test_preflight_happy_path(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("APP_PG_HOST", "127.0.0.1")
    monkeypatch.setenv("APP_PG_PORT", "5432")
    monkeypatch.setenv("APP_PG_DB", "test")
    monkeypatch.setenv("APP_PG_USER", "test")
    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("PHONE_NUMBER", "+10000000000")

    settings = _FakeSettings(tmp_path)
    result = run_preflight_checks("worker", settings_obj=settings, pg_client_factory=_OkPgClient)

    assert result.ok is True
    assert result.role == "worker"


def test_preflight_fails_when_db_ping_is_broken(monkeypatch, tmp_path: Path):
    for key, value in {
        "APP_PG_HOST": "127.0.0.1",
        "APP_PG_PORT": "5432",
        "APP_PG_DB": "test",
        "APP_PG_USER": "test",
    }.items():
        monkeypatch.setenv(key, value)

    settings = _FakeSettings(tmp_path)

    try:
        run_preflight_checks("scheduler", settings_obj=settings, pg_client_factory=_BadPgClient)
        assert False, "Ожидали PreflightError"
    except PreflightError as exc:
        assert "PostgreSQL недоступен" in str(exc)


def test_operational_health_snapshot_detects_role_problems(monkeypatch):
    module, fake_health = _build_health_payload("degraded", {"bot": "ok", "scheduler": "down", "worker": "ok"})
    monkeypatch.setattr(module, "get_system_health", fake_health)

    snapshot = build_operational_snapshot(_FakeRepo({}))

    assert snapshot.overall_status == "degraded"
    assert snapshot.role_problems == ["scheduler"]


def test_smoke_logic_healthy_and_saturated(monkeypatch):
    module, fake_healthy = _build_health_payload("normal", {"bot": "ok", "scheduler": "ok", "worker": "ok"})
    monkeypatch.setattr(module, "get_system_health", fake_healthy)
    healthy = build_operational_snapshot(_FakeRepo({}))
    assert healthy.overall_status == "healthy"

    module, fake_sat = _build_health_payload("saturated", {"bot": "ok", "scheduler": "ok", "worker": "ok"})
    monkeypatch.setattr(module, "get_system_health", fake_sat)
    saturated = build_operational_snapshot(_FakeRepo({}))
    assert saturated.overall_status == "saturated"


def test_split_runtime_contract_keeps_legacy_all_mode():
    assert normalize_runtime_role("ui") == "bot"
    assert normalize_runtime_role("all") == "all"
    assert normalize_runtime_role("unknown") == "all"
