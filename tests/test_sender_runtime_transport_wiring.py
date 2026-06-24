from __future__ import annotations

import asyncio


class _RawBot:
    def __init__(self, *args, **kwargs):
        self.init_args = args
        self.init_kwargs = kwargs
        self.deleted_webhook = False
        self.commands_set = False

    async def delete_webhook(self, *args, **kwargs):
        self.deleted_webhook = True

    async def set_my_commands(self, *args, **kwargs):
        self.commands_set = True


class _RawTelethonClient:
    pass


class _Policy:
    def __init__(self, name: str):
        self.name = name

    async def execute(self, *, backend, key, op_name, func):
        return await func()


def test_sender_runtime_wraps_only_sender_clients(monkeypatch):
    async def scenario():
        import bot as runtime

        raw_telethon_client = _RawTelethonClient()
        reaction_client = object()
        sender_service_kwargs = {}
        builder_calls: list[str] = []
        bot_wrap_calls = []
        telethon_wrap_calls = []

        class _SenderService:
            def __init__(self, **kwargs):
                sender_service_kwargs.update(kwargs)

        def _build_sender_bot_policy():
            builder_calls.append("sender.bot")
            return _Policy("sender.bot.policy")

        def _build_sender_telethon_policy():
            builder_calls.append("sender.telethon")
            return _Policy("sender.telethon.policy")

        original_wrap_bot = runtime.wrap_bot
        original_wrap_telethon_client = runtime.wrap_telethon_client

        def _wrap_bot(raw, *, label, policy):
            bot_wrap_calls.append({"raw": raw, "label": label, "policy": policy})
            return original_wrap_bot(raw, label=label, policy=policy)

        def _wrap_telethon_client(raw, *, label, policy):
            telethon_wrap_calls.append({"raw": raw, "label": label, "policy": policy})
            return original_wrap_telethon_client(raw, label=label, policy=policy)

        async def _create_telethon_client():
            return raw_telethon_client

        async def _create_reaction_clients():
            return [reaction_client]

        monkeypatch.setattr(runtime, "Bot", _RawBot)
        monkeypatch.setattr(runtime, "create_telethon_client", _create_telethon_client)
        monkeypatch.setattr(runtime, "create_reaction_clients", _create_reaction_clients)
        monkeypatch.setattr(runtime, "SenderService", _SenderService)
        monkeypatch.setattr(runtime, "build_sender_bot_policy", _build_sender_bot_policy)
        monkeypatch.setattr(runtime, "build_sender_telethon_policy", _build_sender_telethon_policy)
        monkeypatch.setattr(runtime, "wrap_bot", _wrap_bot)
        monkeypatch.setattr(runtime, "wrap_telethon_client", _wrap_telethon_client)
        monkeypatch.setattr(runtime, "user_handlers_ctx", None)
        monkeypatch.setattr(runtime, "admin_handlers_ctx", None)

        await runtime._init_sender_runtime(create_ui_policy=False)

        raw_bot = runtime.bot
        sender_bot = sender_service_kwargs["bot"]
        sender_telethon = sender_service_kwargs["telethon_client"]

        assert isinstance(raw_bot, _RawBot)
        assert getattr(raw_bot, "_is_transport_proxy", False) is False
        assert getattr(sender_bot, "_is_transport_proxy", False) is True
        assert sender_bot.raw is raw_bot
        assert getattr(sender_telethon, "_is_transport_proxy", False) is True
        assert sender_telethon.raw is raw_telethon_client

        assert sender_service_kwargs["reaction_clients"] == [reaction_client]
        assert runtime.reaction_clients == [reaction_client]
        assert telethon_wrap_calls == [
            {
                "raw": raw_telethon_client,
                "label": "sender.telethon",
                "policy": telethon_wrap_calls[0]["policy"],
            }
        ]
        assert telethon_wrap_calls[0]["policy"].name == "sender.telethon.policy"
        assert bot_wrap_calls[0]["raw"] is raw_bot
        assert bot_wrap_calls[0]["label"] == "sender.bot"
        assert bot_wrap_calls[0]["policy"].name == "sender.bot.policy"
        assert builder_calls == ["sender.bot", "sender.telethon"]
        assert runtime.runtime_context.bot is raw_bot
        assert runtime.runtime_context.telethon_client is raw_telethon_client
    asyncio.run(scenario())
