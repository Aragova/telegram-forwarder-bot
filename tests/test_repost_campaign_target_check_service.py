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
    assert "не имеет права публиковать" in (result.error_text or "")
