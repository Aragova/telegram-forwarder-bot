from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from app.postgres_repository import PostgresRepository


class _Cursor:
    def __init__(self, store):
        self.store = store
        self.rowcount = 0
        self._row = None
        self._rows = []

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        params = params or ()
        self._row = None; self._rows = []; self.rowcount = 0
        if normalized.startswith("INSERT INTO campaign_invite_links("):
            link_hash = params[12]
            existing = next((r for r in self.store["links"].values() if r["invite_link_hash"] == link_hash), None)
            if existing:
                self._row = {"id": existing["id"]}; self.rowcount = 1; return
            row_id = self.store["next_link_id"]; self.store["next_link_id"] += 1
            now = datetime.now(timezone.utc)
            row = dict(zip([
                "rule_id", "campaign_run_id", "campaign_run_message_id", "saved_post_id",
                "destination_chat_id", "destination_chat_title", "ad_target_id", "ad_target_thread_id",
                "ad_target_title", "link_mode", "invite_link", "invite_link_name", "invite_link_hash",
                "creates_join_request", "status", "created_by", "telegram_payload_json",
            ], params))
            row.update({"id": row_id, "created_at": now, "updated_at": now, "revoked_at": None, "archived_at": None})
            self.store["links"][row_id] = row
            self._row = {"id": row_id}; self.rowcount = 1; return
        if normalized.startswith("SELECT * FROM campaign_invite_links WHERE id"):
            self._row = self.store["links"].get(int(params[0])); self.rowcount = 1 if self._row else 0; return
        if normalized.startswith("SELECT * FROM campaign_invite_links WHERE invite_link_hash"):
            self._row = next((r for r in self.store["links"].values() if r["invite_link_hash"] == params[0]), None); return
        if normalized.startswith("SELECT * FROM campaign_invite_links WHERE rule_id"):
            rule_id = int(params[0]); limit = int(params[-1])
            rows = [r for r in self.store["links"].values() if r["rule_id"] == rule_id]
            if "status = ANY" in normalized:
                statuses = set(params[1]); rows = [r for r in rows if r["status"] in statuses]
            self._rows = sorted(rows, key=lambda r: (r["created_at"], r["id"]), reverse=True)[:limit]; return
        if normalized.startswith("UPDATE campaign_invite_links SET status = 'archived'"):
            row = self.store["links"].get(int(params[0]))
            if row:
                row.update({"status": "archived", "archived_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)})
                self.rowcount = 1
            return
        if normalized.startswith("UPDATE campaign_invite_links SET status = 'revoked'"):
            row = self.store["links"].get(int(params[-1]))
            if row:
                row.update({"status": "revoked", "revoked_at": params[0] or datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)})
                if len(params) == 3: row["telegram_payload_json"] = params[1]
                self.rowcount = 1
            return
        if normalized.startswith("UPDATE campaign_invite_links SET campaign_run_message_id"):
            row = self.store["links"].get(int(params[2]))
            if row:
                row.update({"campaign_run_message_id": params[0], "campaign_run_id": params[1], "updated_at": datetime.now(timezone.utc)})
                self.rowcount = 1
            return
        if normalized.startswith("INSERT INTO campaign_invite_link_events("):
            key = (int(params[0]), params[7], params[8])
            existing = next((r for r in self.store["events"].values() if (r["invite_link_id"], r["event_type"], r["telegram_user_id_hash"]) == key), None)
            if existing:
                self._row = {"id": existing["id"]}; self.rowcount = 1; return
            row_id = self.store["next_event_id"]; self.store["next_event_id"] += 1
            now = datetime.now(timezone.utc)
            row = dict(zip([
                "invite_link_id", "rule_id", "campaign_run_id", "campaign_run_message_id", "destination_chat_id",
                "ad_target_id", "ad_target_thread_id", "event_type", "telegram_user_id_hash",
                "telegram_user_payload_json", "telegram_update_id", "event_at", "raw_update_json",
            ], params))
            row["invite_link_id"] = int(row["invite_link_id"]); row["rule_id"] = int(row["rule_id"])
            row.update({"id": row_id, "event_at": row["event_at"] or now, "created_at": now})
            self.store["events"][row_id] = row
            self._row = {"id": row_id}; self.rowcount = 1; return
        if normalized.startswith("SELECT COUNT(*) AS links_total"):
            rows = [r for r in self.store["links"].values() if r["rule_id"] == int(params[0])]
            self._row = {"links_total": len(rows), "links_active": sum(r["status"] == "active" for r in rows)}; return
        if normalized.startswith("SELECT COUNT(*) FILTER (WHERE event_type"):
            rows = [r for r in self.store["events"].values() if r["rule_id"] == int(params[0])]
            self._row = {
                "join_requests_total": sum(r["event_type"] == "join_request_created" for r in rows),
                "joins_total": sum(r["event_type"] == "member_joined" for r in rows),
                "left_total": sum(r["event_type"] == "member_left" for r in rows),
                "kicked_total": sum(r["event_type"] == "member_kicked" for r in rows),
                "unknown_total": sum(r["event_type"] == "member_unknown" for r in rows),
                "unique_users_total": len({r["telegram_user_id_hash"] for r in rows}),
            }; return
        raise AssertionError(f"Unexpected SQL: {normalized}")

    def fetchone(self): return self._row
    def fetchall(self): return self._rows


class _Conn:
    def __init__(self, store): self.store = store
    def cursor(self): return _Cursor(self.store)
    def commit(self): return None


@contextmanager
def _connect(store): yield _Conn(store)


def _repo():
    store = {"links": {}, "events": {}, "next_link_id": 1, "next_event_id": 1}
    repo = PostgresRepository(); repo.connect = lambda: _connect(store)
    return repo, store


def _create(repo, rule_id=10, link_hash="hash", status="active"):
    return repo.create_campaign_invite_link_record(
        rule_id=rule_id, destination_chat_id="-100", invite_link=f"https://t.me/+{link_hash}",
        invite_link_hash=link_hash, link_mode="join_request", creates_join_request=True, status=status,
    )


def test_create_and_get_link():
    repo, _ = _repo(); link_id = _create(repo)
    row = repo.get_campaign_invite_link(link_id)
    assert row["rule_id"] == 10
    assert row["destination_chat_id"] == "-100"
    assert row["invite_link"] == "https://t.me/+hash"
    assert row["invite_link_hash"] == "hash"
    assert row["link_mode"] == "join_request"
    assert row["creates_join_request"] is True
    assert row["status"] == "active"


def test_get_by_hash():
    repo, _ = _repo(); link_id = _create(repo, link_hash="hash2")
    assert repo.get_campaign_invite_link_by_hash("hash2")["id"] == link_id


def test_duplicate_hash_returns_existing_id_without_second_row():
    repo, store = _repo(); first = _create(repo); second = _create(repo, rule_id=20)
    assert second == first
    assert len(store["links"]) == 1


def test_list_for_rule():
    repo, _ = _repo(); _create(repo, 10, "a"); _create(repo, 10, "b"); _create(repo, 11, "c")
    rows = repo.list_campaign_invite_links_for_rule(10)
    assert len(rows) == 2
    assert {r["rule_id"] for r in rows} == {10}


def test_filter_statuses():
    repo, _ = _repo(); active = _create(repo, 10, "a"); archived = _create(repo, 10, "b", "archived"); revoked = _create(repo, 10, "c", "revoked")
    assert [r["id"] for r in repo.list_campaign_invite_links_for_rule(10, statuses=["active"])] == [active]
    assert {r["id"] for r in repo.list_campaign_invite_links_for_rule(10, statuses=["archived", "revoked"])} == {archived, revoked}


def test_archive_link():
    repo, _ = _repo(); link_id = _create(repo)
    assert repo.archive_campaign_invite_link(link_id) is True
    row = repo.get_campaign_invite_link(link_id)
    assert row["status"] == "archived"
    assert row["archived_at"] is not None


def test_revoke_link():
    repo, _ = _repo(); link_id = _create(repo)
    assert repo.mark_campaign_invite_link_revoked(link_id) is True
    row = repo.get_campaign_invite_link(link_id)
    assert row["status"] == "revoked"
    assert row["revoked_at"] is not None


def test_bind_to_run_message():
    repo, _ = _repo(); link_id = _create(repo)
    assert repo.bind_campaign_invite_link_to_run_message(link_id, campaign_run_message_id=55, campaign_run_id=44) is True
    row = repo.get_campaign_invite_link(link_id)
    assert row["campaign_run_id"] == 44
    assert row["campaign_run_message_id"] == 55


def test_create_event():
    repo, store = _repo(); link_id = _create(repo)
    event_id = repo.create_campaign_invite_link_event(invite_link_id=link_id, rule_id=10, destination_chat_id="-100", event_type="join_request_created", telegram_user_id_hash="u1")
    assert store["events"][event_id]["event_type"] == "join_request_created"


def test_deduplicate_event():
    repo, store = _repo(); link_id = _create(repo)
    first = repo.create_campaign_invite_link_event(invite_link_id=link_id, rule_id=10, destination_chat_id="-100", event_type="join_request_created", telegram_user_id_hash="u1")
    second = repo.create_campaign_invite_link_event(invite_link_id=link_id, rule_id=10, destination_chat_id="-100", event_type="join_request_created", telegram_user_id_hash="u1")
    assert second == first
    assert len(store["events"]) == 1


def test_stats_for_rule():
    repo, _ = _repo(); a = _create(repo, 10, "a"); b = _create(repo, 10, "b")
    events = ["join_request_created"] * 3 + ["member_joined"] * 2 + ["member_left", "member_kicked"]
    for idx, event_type in enumerate(events):
        repo.create_campaign_invite_link_event(invite_link_id=a if idx % 2 else b, rule_id=10, destination_chat_id="-100", event_type=event_type, telegram_user_id_hash=f"u{idx}")
    stats = repo.get_campaign_invite_link_stats_for_rule(10)
    assert stats == {"rule_id": 10, "links_total": 2, "links_active": 2, "join_requests_total": 3, "joins_total": 2, "left_total": 1, "kicked_total": 1, "unknown_total": 0, "unique_users_total": 7}


def test_invalid_values_are_rejected():
    repo, store = _repo()
    assert repo.create_campaign_invite_link_record(rule_id=10, destination_chat_id="-100", invite_link="x", invite_link_hash="bad1", link_mode="bad", creates_join_request=True) is None
    assert repo.create_campaign_invite_link_record(rule_id=10, destination_chat_id="-100", invite_link="x", invite_link_hash="bad2", link_mode="join_request", creates_join_request=True, status="bad") is None
    assert repo.create_campaign_invite_link_event(invite_link_id=1, rule_id=10, destination_chat_id="-100", event_type="bad", telegram_user_id_hash="u") is None
    assert store["links"] == {}
    assert store["events"] == {}
