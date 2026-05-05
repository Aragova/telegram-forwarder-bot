import asyncio

from app.repost_campaign_delete_service import RepostCampaignDeleteService


class _Bot:
    def __init__(self, fail=False):
        self.fail = fail

    async def delete_message(self, **kwargs):
        if self.fail:
            raise RuntimeError("bot failed")


class _Telethon:
    def __init__(self, fail=False):
        self.fail = fail

    async def delete_messages(self, entity, ids):
        if self.fail:
            raise RuntimeError("telethon failed")


def test_delete_bot_api_success():
    service = RepostCampaignDeleteService(bot=_Bot(), telethon_client=_Telethon())
    result = asyncio.run(service.delete_message(target_id="-1001", message_id=10))
    assert result.ok is True
    assert result.method == "bot_api"


def test_delete_telethon_success_for_builder():
    service = RepostCampaignDeleteService(bot=_Bot(fail=True), telethon_client=_Telethon())
    result = asyncio.run(service.delete_message(target_id="-1001", message_id=10, render_mode="telethon_builder"))
    assert result.ok is True
    assert result.method == "telethon"


def test_delete_fallback_bot_api_to_telethon():
    service = RepostCampaignDeleteService(bot=_Bot(fail=True), telethon_client=_Telethon())
    result = asyncio.run(service.delete_message(target_id="-1001", message_id=10))
    assert result.ok is True
    assert result.method == "telethon"


def test_delete_both_failed():
    service = RepostCampaignDeleteService(bot=_Bot(fail=True), telethon_client=_Telethon(fail=True))
    result = asyncio.run(service.delete_message(target_id="-1001", message_id=10))
    assert result.ok is False
    assert result.method == "failed"
    assert "Bot API" in (result.error_text or "")
    assert "Telethon" in (result.error_text or "")


def test_delete_messages_bot_api_all_ids():
    calls = []
    class B(_Bot):
        async def delete_message(self, **kwargs):
            calls.append(kwargs["message_id"])
    service = RepostCampaignDeleteService(bot=B(), telethon_client=_Telethon())
    result = asyncio.run(service.delete_messages(target_id="-1001", message_ids=[1,2,3]))
    assert result.ok is True
    assert calls == [1,2,3]


def test_delete_messages_telethon_for_builder():
    calls = []
    class T(_Telethon):
        async def delete_messages(self, entity, ids):
            calls.append(ids)
    service = RepostCampaignDeleteService(bot=_Bot(), telethon_client=T())
    result = asyncio.run(service.delete_messages(target_id="-1001", message_ids=[4,5], render_mode="telethon_builder"))
    assert result.ok is True
    assert calls == [[4,5]]
