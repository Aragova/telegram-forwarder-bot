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
    def __init__(self):
        self.handlers = []
        self.caught_up = False

    async def get_me(self):
        from types import SimpleNamespace
        return SimpleNamespace(id=12345)

    def add_event_handler(self, callback, event):
        self.handlers.append((callback, event))

    async def catch_up(self):
        assert self.handlers
        self.caught_up = True


class _Policy:
    def __init__(self, name: str):
        self.name = name

    async def execute(self, *, backend, key, op_name, func):
        return await func()


def test_sender_runtime_wraps_only_sender_clients(monkeypatch):
    async def scenario():
        import bot as runtime

        raw_telethon_client = _RawTelethonClient()
        from types import SimpleNamespace
        repository = SimpleNamespace(record_telegram_publication_mappings=lambda *args: None)
        monkeypatch.setattr(runtime, "db", repository)
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

        async def _create_telethon_client(*, publication_repository=None):
            from app.telegram_publication import prepare_publication_inbox
            assert publication_repository is repository
            bind=prepare_publication_inbox(raw_telethon_client,publication_repository)
            await bind()
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
        assert raw_bot.init_kwargs["session"].api.is_local is True
        assert raw_bot.init_kwargs["session"].api.api_url("123456:ABC", "sendVideo").startswith(runtime.settings.bot_api_base)
        assert "base_url" not in raw_bot.init_kwargs
        sender_bot = sender_service_kwargs["bot"]
        sender_telethon = sender_service_kwargs["telethon_client"]

        assert isinstance(raw_bot, _RawBot)
        assert getattr(raw_bot, "_is_transport_proxy", False) is False
        assert getattr(sender_bot, "_is_transport_proxy", False) is True
        assert sender_bot.raw is raw_bot
        assert getattr(sender_telethon, "_is_transport_proxy", False) is True
        assert sender_telethon.raw is raw_telethon_client
        assert sender_service_kwargs["telethon_read_client"] is sender_telethon
        assert sender_telethon.publication_inbox is raw_telethon_client.publication_inbox
        assert sender_telethon.publication_inbox.sender_account_id == 12345
        assert len(raw_telethon_client.handlers) == 1
        assert not raw_telethon_client.caught_up

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



def test_actual_sender_factory_captures_connect_events_and_retries_startup_write(monkeypatch):
    from types import SimpleNamespace

    from telethon import types, utils
    from app import telegram_client

    peer = types.PeerChannel(99)

    class Client:
        def __init__(self, *args, **kwargs):
            self.handlers = []

        def add_event_handler(self, callback, event):
            self.handlers.append(callback)

        async def connect(self):
            assert len(self.handlers) == 1
            await self.handlers[0](types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[1], sent_messages=[101],
            ))
            await self.handlers[0](types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[2], sent_messages=[102],
            ))

        async def get_me(self):
            return SimpleNamespace(id=7)

    class Repo:
        def __init__(self):
            self.calls = []
            self.saved = []

        def record_telegram_publication_mappings(self, account, peer_id, pairs):
            self.calls.append(pairs)
            if len(self.calls) == 2:
                raise RuntimeError("temporary PostgreSQL failure")
            self.saved.append((account, peer_id, pairs))

    monkeypatch.setattr(telegram_client, "TelegramClient", Client)
    repo = Repo()
    client = asyncio.run(telegram_client.create_telethon_client(publication_repository=repo))
    assert client.publication_inbox.sender_account_id == 7
    assert len(client.handlers) == 1
    assert repo.calls == [[(1,101)], [(2,102)], [(2,102)]]
    assert repo.saved == [(7,utils.get_peer_id(peer),[(1,101)]),
                          (7,utils.get_peer_id(peer),[(2,102)])]
