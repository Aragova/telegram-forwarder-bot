import asyncio
from types import SimpleNamespace

import pytest

from app.campaign_invite_link_events_service import (
    build_telegram_user_id_hash,
    sanitize_telegram_user_payload,
    CampaignInviteLinkEventsService,
)
from app.campaign_invite_links_service import build_invite_link_hash


INVITE = "https://t.me/+tracked"


class RepoStub:
    def __init__(self, link=None):
        self.link = link
        self.hashes = []
        self.events = []
        self.last_event = None
        self.latest_event_lookups = []

    def get_campaign_invite_link_by_hash(self, invite_link_hash):
        self.hashes.append(invite_link_hash)
        if self.link and invite_link_hash == build_invite_link_hash(INVITE):
            return self.link
        return None

    def create_campaign_invite_link_event(self, **kwargs):
        self.events.append(kwargs)
        return len(self.events)

    def get_latest_campaign_invite_link_event_for_user(self, **kwargs):
        self.latest_event_lookups.append(kwargs)
        return self.last_event


def link(status="active"):
    return {"id": 7, "rule_id": 10, "destination_chat_id": "-100", "status": status, "campaign_run_id": 1, "campaign_run_message_id": 2, "ad_target_id": "a", "ad_target_thread_id": 3}


def user(uid=123):
    return SimpleNamespace(id=uid, first_name="Ann", username="ann", is_bot=False)


def join_request(invite=INVITE):
    invite_obj = SimpleNamespace(invite_link=invite) if invite is not None else None
    return SimpleNamespace(invite_link=invite_obj, from_user=user(), update_id=55, date=None, chat=SimpleNamespace(id="-100"))


def member_update(old="left", new="member", invite=INVITE):
    invite_obj = SimpleNamespace(invite_link=invite) if invite is not None else None
    return SimpleNamespace(
        invite_link=invite_obj,
        old_chat_member=SimpleNamespace(status=old, user=user()),
        new_chat_member=SimpleNamespace(status=new, user=user()),
        update_id=56,
        date=None,
        chat=SimpleNamespace(id="-100"),
    )


def test_build_telegram_user_id_hash_is_stable_and_normalizes_str_int():
    assert build_telegram_user_id_hash(123) == build_telegram_user_id_hash("123")
    assert build_telegram_user_id_hash(" 123 ") == build_telegram_user_id_hash("123")


def test_build_telegram_user_id_hash_rejects_empty():
    with pytest.raises(ValueError):
        build_telegram_user_id_hash("   ")


def test_sanitize_telegram_user_payload_excludes_user_id():
    payload = sanitize_telegram_user_payload(SimpleNamespace(id=1, first_name="Ann", username="ann", telegram_id=2, user_id=3))
    assert payload["first_name"] == "Ann"
    assert payload["username"] == "ann"
    assert "id" not in payload
    assert "telegram_id" not in payload
    assert "user_id" not in payload


def test_join_request_missing_invite_link_skips_without_repo_lookup():
    repo = RepoStub(link())
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_join_request(join_request(None)))
    assert result["skipped"] is True
    assert result["reason"] == "missing_invite_link"
    assert repo.hashes == []


def test_join_request_unknown_invite_link_skips_without_event():
    repo = RepoStub(None)
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_join_request(join_request()))
    assert result["reason"] == "invite_link_not_tracked"
    assert repo.events == []


def test_join_request_active_link_records_event_with_hash_only():
    repo = RepoStub(link())
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_join_request(join_request()))
    assert result["ok"] is True
    event = repo.events[0]
    assert event["event_type"] == "join_request_created"
    assert event["telegram_user_id_hash"] == build_telegram_user_id_hash(123)
    assert "id" not in event["telegram_user_payload_json"]
    assert "from_user" not in event["raw_update_json"]


def test_join_request_revoked_link_skips_without_event():
    repo = RepoStub(link("revoked"))
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_join_request(join_request()))
    assert result["reason"] == "invite_link_not_active"
    assert repo.events == []


def test_chat_member_missing_invite_link_skips():
    repo = RepoStub(link())
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_member_updated(member_update(invite=None)))
    assert result["reason"] == "missing_invite_link"
    assert repo.events == []


@pytest.mark.parametrize(("old", "new", "event_type"), [("left", "member", "member_joined"), ("member", "left", "member_left"), ("member", "kicked", "member_kicked"), ("member", "restricted", "member_unknown")])
def test_chat_member_event_types(old, new, event_type):
    repo = RepoStub(link())
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_member_updated(member_update(old, new)))
    assert result["event_type"] == event_type
    assert repo.events[0]["event_type"] == event_type


def test_dedup_is_repository_owned_and_repeated_calls_do_not_fail():
    repo = RepoStub(link())
    service = CampaignInviteLinkEventsService(repo=repo)
    first = asyncio.run(service.handle_chat_member_updated(member_update()))
    second = asyncio.run(service.handle_chat_member_updated(member_update()))
    assert first["ok"] is True
    assert second["ok"] is True


def last_event():
    return {
        "id": 99,
        "invite_link_id": 8,
        "rule_id": 11,
        "campaign_run_id": 12,
        "campaign_run_message_id": 13,
        "destination_chat_id": "-100",
        "ad_target_id": "target",
        "ad_target_thread_id": 14,
        "event_type": "member_joined",
        "telegram_user_id_hash": build_telegram_user_id_hash(123),
        "event_at": None,
    }


def test_chat_member_left_without_invite_link_resolves_by_latest_user_event():
    repo = RepoStub(link())
    repo.last_event = last_event()
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_member_updated(member_update("member", "left", invite=None)))
    assert result["ok"] is True
    assert result["event_type"] == "member_left"
    assert result["resolved_by"] == "latest_user_event"
    assert result["invite_link_id"] == 8
    assert repo.events[0]["invite_link_id"] == 8
    assert repo.events[0]["event_type"] == "member_left"
    assert repo.latest_event_lookups[0]["telegram_user_id_hash"] == build_telegram_user_id_hash(123)
    assert repo.latest_event_lookups[0]["event_types"] == ["member_joined", "join_request_created"]


def test_chat_member_kicked_without_invite_link_resolves_by_latest_user_event():
    repo = RepoStub(link())
    repo.last_event = last_event()
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_member_updated(member_update("member", "kicked", invite=None)))
    assert result["ok"] is True
    assert result["event_type"] == "member_kicked"
    assert result["resolved_by"] == "latest_user_event"
    assert repo.events[0]["invite_link_id"] == 8
    assert repo.events[0]["event_type"] == "member_kicked"


def test_chat_member_left_without_invite_link_and_without_last_event_skips():
    repo = RepoStub(link())
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_member_updated(member_update("member", "left", invite=None)))
    assert result["reason"] == "tracked_user_event_not_found"
    assert repo.events == []


def test_chat_member_joined_without_invite_link_still_skips():
    repo = RepoStub(link())
    result = asyncio.run(CampaignInviteLinkEventsService(repo=repo).handle_chat_member_updated(member_update("left", "member", invite=None)))
    assert result["reason"] == "missing_invite_link"
    assert repo.events == []
