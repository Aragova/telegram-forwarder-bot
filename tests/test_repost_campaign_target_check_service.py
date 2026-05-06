import asyncio
from types import SimpleNamespace

from app.repost_campaign_target_check_service import RepostCampaignTargetCheckService


class _FakeTelethon:
    def __init__(self, *, entity=None, error=False, permissions=None):
        self.entity = entity
        self.error = error
        self.permissions = permissions

    async def get_entity(self, _):
        if self.error:
            raise RuntimeError("no access")
        return self.entity

    async def get_permissions(self, _entity, _me):
        return self.permissions


def test_entity_not_found():
    svc = RepostCampaignTargetCheckService(telethon_client=_FakeTelethon(error=True))
    result = asyncio.run(svc.check_target(target_id="-1001"))
    assert result.ok is False
    assert "не видит" in (result.error_text or "")


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
