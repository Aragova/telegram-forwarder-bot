import asyncio

import pytest

import app.campaign_invite_links_service as service_module
from app.campaign_invite_links_service import (
    CampaignInviteLinksService,
    INVITE_LINK_MODE_DIRECT_JOIN,
    INVITE_LINK_MODE_JOIN_REQUEST,
    build_invite_link_hash,
    build_invite_link_name,
)


class FakeTelegramError(Exception):
    pass


class FakeForbiddenError(FakeTelegramError):
    pass


class FakeBadRequestError(FakeTelegramError):
    pass


class FakeAPIError(FakeTelegramError):
    pass


class FakeBot:
    def __init__(self, *, create_result=None, create_error=None, revoke_result=None, revoke_error=None):
        self.create_result = create_result or {"invite_link": "https://t.me/+ABC", "name": "test", "creates_join_request": True}
        self.create_error = create_error
        self.revoke_result = revoke_result or {"invite_link": "https://t.me/+ABC", "name": "test"}
        self.revoke_error = revoke_error
        self.create_calls = []
        self.revoke_calls = []

    async def create_chat_invite_link(self, **kwargs):
        self.create_calls.append(kwargs)
        if self.create_error:
            raise self.create_error
        return self.create_result

    async def revoke_chat_invite_link(self, **kwargs):
        self.revoke_calls.append(kwargs)
        if self.revoke_error:
            raise self.revoke_error
        return self.revoke_result


class FakeRepo:
    def __init__(self, *, create_id=123, link=None):
        self.create_id = create_id
        self.link = link
        self.create_calls = []
        self.get_calls = []
        self.revoke_calls = []

    def create_campaign_invite_link_record(self, **kwargs):
        self.create_calls.append(kwargs)
        return self.create_id

    def get_campaign_invite_link(self, invite_link_id):
        self.get_calls.append(invite_link_id)
        return self.link

    def mark_campaign_invite_link_revoked(self, invite_link_id, **kwargs):
        self.revoke_calls.append((invite_link_id, kwargs))
        return True


@pytest.fixture(autouse=True)
def fake_telegram_exceptions(monkeypatch):
    monkeypatch.setattr(service_module, "TelegramForbiddenError", FakeForbiddenError)
    monkeypatch.setattr(service_module, "TelegramBadRequest", FakeBadRequestError)
    monkeypatch.setattr(service_module, "TelegramAPIError", FakeAPIError)


def test_build_invite_link_hash_strips_spaces():
    assert build_invite_link_hash(" https://t.me/+ABC ") == build_invite_link_hash("https://t.me/+ABC")


def test_build_invite_link_hash_empty_raises():
    with pytest.raises(ValueError):
        build_invite_link_hash("   ")


def test_build_invite_link_name_rule_and_run_and_long_target():
    rule_name = build_invite_link_name(rule_id=123)
    run_name = build_invite_link_name(rule_id=123, campaign_run_id=456)
    target_name = build_invite_link_name(rule_id=123, campaign_run_id=456, ad_target_title="Очень длинное название таргета")

    assert rule_name
    assert "run #456" in run_name
    assert len(rule_name) <= 32
    assert len(run_name) <= 32
    assert len(target_name) <= 32
    assert target_name


def test_create_join_request_link():
    bot = FakeBot(create_result={"invite_link": "https://t.me/+JOIN", "name": "join", "creates_join_request": True})
    repo = FakeRepo(create_id=7)
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).create_invite_link(
        rule_id=10,
        destination_chat_id="-100",
        destination_chat_title="Channel",
        link_mode=INVITE_LINK_MODE_JOIN_REQUEST,
    ))

    assert result["ok"] is True
    assert bot.create_calls[0]["chat_id"] == "-100"
    assert bot.create_calls[0]["creates_join_request"] is True
    assert repo.create_calls[0]["link_mode"] == "join_request"
    assert repo.create_calls[0]["creates_join_request"] is True
    assert repo.create_calls[0]["status"] == "active"


def test_create_direct_join_link():
    bot = FakeBot(create_result={"invite_link": "https://t.me/+DIRECT", "name": "direct", "creates_join_request": False})
    repo = FakeRepo(create_id=8)
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).create_invite_link(
        rule_id=10,
        destination_chat_id="-100",
        destination_chat_title=None,
        link_mode=INVITE_LINK_MODE_DIRECT_JOIN,
    ))

    assert result["ok"] is True
    assert bot.create_calls[0]["creates_join_request"] is False
    assert repo.create_calls[0]["link_mode"] == "direct_join"
    assert repo.create_calls[0]["creates_join_request"] is False


def test_create_invalid_link_mode_does_not_call_bot_or_repo():
    bot = FakeBot()
    repo = FakeRepo()
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).create_invite_link(
        rule_id=10,
        destination_chat_id="-100",
        destination_chat_title=None,
        link_mode="bad",
    ))

    assert result["ok"] is False
    assert result["error_code"] == "invalid_link_mode"
    assert bot.create_calls == []
    assert repo.create_calls == []


def test_create_empty_destination_does_not_call_bot_or_repo():
    bot = FakeBot()
    repo = FakeRepo()
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).create_invite_link(
        rule_id=10,
        destination_chat_id=" ",
        destination_chat_title=None,
        link_mode=INVITE_LINK_MODE_JOIN_REQUEST,
    ))

    assert result["ok"] is False
    assert result["error_code"] == "destination_chat_required"
    assert bot.create_calls == []
    assert repo.create_calls == []


def test_create_repository_save_failed_returns_invite_link():
    bot = FakeBot(create_result={"invite_link": "https://t.me/+SAVED", "name": "saved"})
    repo = FakeRepo(create_id=None)
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).create_invite_link(
        rule_id=10,
        destination_chat_id="-100",
        destination_chat_title=None,
        link_mode=INVITE_LINK_MODE_JOIN_REQUEST,
    ))

    assert result["ok"] is False
    assert result["error_code"] == "repository_save_failed"
    assert result["invite_link"] == "https://t.me/+SAVED"


def test_create_telegram_permission_denied():
    bot = FakeBot(create_error=FakeForbiddenError("not enough rights to create invite link"))
    repo = FakeRepo()
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).create_invite_link(
        rule_id=10,
        destination_chat_id="-100",
        destination_chat_title=None,
        link_mode=INVITE_LINK_MODE_JOIN_REQUEST,
    ))

    assert result["ok"] is False
    assert result["error_code"] == "telegram_permission_denied"
    assert repo.create_calls == []


def test_create_telegram_chat_unavailable():
    bot = FakeBot(create_error=FakeBadRequestError("chat not found"))
    repo = FakeRepo()
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).create_invite_link(
        rule_id=10,
        destination_chat_id="-100",
        destination_chat_title=None,
        link_mode=INVITE_LINK_MODE_JOIN_REQUEST,
    ))

    assert result["ok"] is False
    assert result["error_code"] == "telegram_chat_not_available"
    assert repo.create_calls == []


def test_revoke_invite_link():
    link = {"id": 5, "destination_chat_id": "-100", "invite_link": "https://t.me/+ABC", "status": "active"}
    bot = FakeBot()
    repo = FakeRepo(link=link)
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).revoke_invite_link(invite_link_id=5))

    assert result["ok"] is True
    assert result["revoked"] is True
    assert bot.revoke_calls == [{"chat_id": "-100", "invite_link": "https://t.me/+ABC"}]
    assert repo.revoke_calls[0][0] == 5


def test_revoke_already_revoked_in_db():
    link = {"id": 5, "destination_chat_id": "-100", "invite_link": "https://t.me/+ABC", "status": "revoked"}
    bot = FakeBot()
    repo = FakeRepo(link=link)
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).revoke_invite_link(invite_link_id=5))

    assert result["ok"] is True
    assert result["already_revoked"] is True
    assert bot.revoke_calls == []
    assert repo.revoke_calls == []


def test_revoke_unknown_link():
    bot = FakeBot()
    repo = FakeRepo(link=None)
    result = asyncio.run(CampaignInviteLinksService(repo=repo, bot=bot).revoke_invite_link(invite_link_id=999))

    assert result["ok"] is False
    assert result["error_code"] == "invite_link_not_found"
    assert bot.revoke_calls == []
