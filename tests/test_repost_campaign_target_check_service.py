import asyncio
from types import SimpleNamespace

from app.repost_campaign_target_check_service import RepostCampaignTargetCheckService


class _FakeTelethon:
    def __init__(self, *, entity=None, error=False, permissions=None, permissions_error=False):
        self.entity = entity
        self.error = error
        self.permissions = permissions
        self.permissions_error = permissions_error

    async def get_entity(self, _):
        if self.error:
            raise RuntimeError("no access")
        return self.entity

    async def get_permissions(self, _entity, _me):
        if self.permissions_error:
            raise RuntimeError("permissions error")
        return self.permissions


class _FakeBot:
    def __init__(self, *, me_id=101, chat_type="channel", member=None, chat_error=False, member_error=False):
        self.me_id = me_id
        self.chat_type = chat_type
        self.member = member or SimpleNamespace(status="administrator", can_post_messages=True, can_delete_messages=True)
        self.chat_error = chat_error
        self.member_error = member_error

    async def get_me(self):
        return SimpleNamespace(id=self.me_id)

    async def get_chat(self, _target_id):
        if self.chat_error:
            raise RuntimeError("chat error")
        return SimpleNamespace(type=self.chat_type)

    async def get_chat_member(self, _target_id, _bot_id):
        if self.member_error:
            raise RuntimeError("member error")
        return self.member


def test_entity_not_found():
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(error=True))
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.ok is False
    assert result.publish_status == "unknown"


def test_publish_rights_ok():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=SimpleNamespace(post_messages=True, delete_messages=True))
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=SimpleNamespace(title="Channel A"), permissions=perms))
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.ok is True
    assert result.can_publish is True
    assert result.title == "Channel A"


def test_no_publish_rights():
    perms = SimpleNamespace(is_admin=False, is_creator=False, admin_rights=None)
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=SimpleNamespace(title="Channel A"), permissions=perms))
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.ok is False
    assert "ViMi" in (result.error_text or "")


def test_broadcast_admin_without_post_messages_fails():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=SimpleNamespace(post_messages=False))
    entity = SimpleNamespace(title="Channel A", broadcast=True, megagroup=False)
    result = asyncio.run(RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms)).check_target(target_id="-1001"))
    assert result.ok is False and result.can_publish is False


def test_broadcast_admin_without_admin_rights_is_allowed():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=None, banned_rights=None)
    entity = SimpleNamespace(title="Channel A", broadcast=True, megagroup=False)
    result = asyncio.run(RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms)).check_target(target_id="-1001"))
    assert result.ok is True
    assert result.can_publish is True
    assert result.can_delete is None
    assert result.error_text is None


def test_megagroup_admin_without_post_messages_is_ok():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=SimpleNamespace(delete_messages=True), banned_rights=None)
    entity = SimpleNamespace(title="Group A", broadcast=False, megagroup=True)
    result = asyncio.run(RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms)).check_target(target_id="-1001"))
    assert result.ok is True and result.can_publish is True


def test_creator_always_ok():
    perms = SimpleNamespace(is_admin=False, is_creator=True, admin_rights=None)
    entity = SimpleNamespace(title="A", broadcast=True)
    result = asyncio.run(RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms)).check_target(target_id="-1001"))
    assert result.ok is True


def test_group_admin_without_admin_rights_is_ok():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=None, banned_rights=None)
    entity = SimpleNamespace(title="Group B", broadcast=False, megagroup=False)
    result = asyncio.run(RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms)).check_target(target_id="-1001"))
    assert result.ok is True


def test_banned_send_messages_fails():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=None, banned_rights=SimpleNamespace(send_messages=True))
    entity = SimpleNamespace(title="Group C", broadcast=False, megagroup=True)
    result = asyncio.run(RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms)).check_target(target_id="-1001"))
    assert result.ok is False and result.can_publish is False


def test_telethon_entity_failed_but_bot_api_confirms_permissions():
    bot = _FakeBot(member=SimpleNamespace(status="administrator", can_post_messages=True, can_delete_messages=True), chat_type="channel")
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(error=True), bot=bot)
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.ok is True
    assert result.publish_status == "confirmed"
    assert result.delete_status == "confirmed"
    assert result.title == "-1001"


def test_telethon_confirmed_bot_api_denied_does_not_return_confirmed():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=SimpleNamespace(post_messages=True, delete_messages=True), banned_rights=None)
    entity = SimpleNamespace(title="Channel A", broadcast=True, megagroup=False)
    bot = _FakeBot(member=SimpleNamespace(status="member", can_post_messages=False, can_delete_messages=False), chat_type="channel")
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms), bot=bot)
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.publish_status == "unknown"
    assert result.ok is False


def test_bot_api_exception_falls_back_to_telethon():
    perms = SimpleNamespace(is_admin=True, is_creator=False, admin_rights=SimpleNamespace(post_messages=True, delete_messages=True), banned_rights=None)
    entity = SimpleNamespace(title="Channel A", broadcast=True, megagroup=False)
    bot = _FakeBot(chat_error=True)
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions=perms), bot=bot)
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.ok is True
    assert result.publish_status == "confirmed"
    assert result.delete_status == "confirmed"


def test_bot_api_channel_admin_without_can_post_is_unknown():
    entity = SimpleNamespace(title="Channel A", broadcast=False, megagroup=False)
    bot = _FakeBot(member=SimpleNamespace(status="administrator", can_post_messages=None, can_delete_messages=True), chat_type="channel")
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(entity=entity, permissions_error=True), bot=bot)
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.publish_status == "unknown"
