from types import SimpleNamespace

from app.top_time_guard_service import TopTimeGuardService


class FakeLogger:
    def __init__(self):
        self.warnings = []

    def warning(self, *args):
        self.warnings.append(args)


class FakeRepo:
    def __init__(self, pause=None, error=None):
        self.pause = pause
        self.error = error
        self.calls = []

    def get_active_campaign_top_time_pause_for_target(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.pause


def test_guard_allows_when_no_active_pause():
    repo = FakeRepo(None)
    rule = SimpleNamespace(id=1, target_id="-1001", target_thread_id=None)

    decision = TopTimeGuardService(repo).build_guard_decision(rule, at_iso="2026-01-01T00:00:00+00:00")

    assert decision["blocked"] is False
    assert decision["reason"] is None


def test_guard_blocks_when_active_pause_exists():
    pause = {"id": 10, "target_id": "-1001", "target_thread_id": None, "ends_at": "2026-01-01T01:00:00+00:00"}
    repo = FakeRepo(pause)
    rule = SimpleNamespace(id=1, target_id="-1001", target_thread_id=None)

    decision = TopTimeGuardService(repo).build_guard_decision(rule)

    assert decision["blocked"] is True
    assert decision["reason"] == "top_time_pause"
    assert decision["resume_at"] == pause["ends_at"]
    assert decision["pause"] == pause


def test_guard_respects_thread_id():
    repo = FakeRepo(None)
    rule = SimpleNamespace(id=1, target_id="-1001", target_thread_id=123)

    TopTimeGuardService(repo).build_guard_decision(rule, at_iso="now")

    assert repo.calls == [{"target_id": "-1001", "target_thread_id": 123, "at_iso": "now"}]


def test_guard_fail_open_on_repo_error():
    logger = FakeLogger()
    repo = FakeRepo(error=RuntimeError("boom"))
    rule = SimpleNamespace(id=1, target_id="-1001", target_thread_id=None)

    decision = TopTimeGuardService(repo, logger=logger).build_guard_decision(rule)

    assert decision["blocked"] is False
    assert logger.warnings
