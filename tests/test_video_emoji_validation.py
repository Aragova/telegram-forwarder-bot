from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon import functions, types
from telethon.errors import DocumentInvalidError, FloodWaitError

from app import video_emoji_validation as validation
from app.send_intent import observe_diagnostic_publication

CHAT = -1003812542665
BAD_ID = 5319302290127991242


def entity(document_id, offset, length=2):
    return {"type": "custom_emoji", "offset": offset, "length": length,
            "custom_emoji_id": str(document_id)}


def document(document_id, alt):
    return types.Document(
        id=document_id, access_hash=1, file_reference=b"", date=datetime.now(timezone.utc),
        mime_type="application/x-tgsticker", size=1, dc_id=1,
        attributes=[types.DocumentAttributeCustomEmoji(alt=alt, stickerset=types.InputStickerSetEmpty())],
    )


def rejection():
    return DocumentInvalidError(request=functions.messages.SendMediaRequest(
        peer=types.InputPeerSelf(), media=types.InputMediaEmpty(), message="", random_id=1,
    ))


class FakeRepo:
    def __init__(self):
        self.inflight = False
        self.reservation = "reserved"
        self.waits = {}
        self.releases = 0

    def reserve_video_emoji_check(self, account_id, user_id, chat_id):
        assert (account_id, user_id, chat_id) == (42, 7, CHAT)
        if self.inflight:
            return "pending_review"
        if self.reservation == "reserved":
            self.inflight = True
        return self.reservation

    def release_video_emoji_check(self, account_id):
        assert account_id == 42
        self.inflight = False
        self.releases += 1

    def get_transport_cooldown(self, scope):
        return self.waits.get(scope, 0)

    def extend_transport_cooldown(self, scope, seconds):
        self.waits[scope] = seconds


class FakeClient:
    def __init__(self, docs, *, bad=(), strip=(), control_rejected=False, uncertain=False):
        self.docs = docs
        self.bad = set(bad)
        self.strip = set(strip)
        self.control_rejected = control_rejected
        self.uncertain = uncertain
        self.calls = []
        self.messages = {}
        self.deleted = []
        self.wrong_identity = False
        self.wrong_text = False

    async def get_me(self, *, input_peer):
        assert input_peer
        return types.InputPeerUser(user_id=42, access_hash=123)

    async def __call__(self, request):
        assert isinstance(request, functions.messages.GetCustomEmojiDocumentsRequest)
        return [doc for doc in self.docs if doc.id in request.document_id]

    async def send_file(self, **kwargs):
        import inspect
        from telethon import TelegramClient

        parameters = inspect.signature(TelegramClient.send_file).parameters
        # Do not allow the fake to silently accept a keyword that is not
        # explicitly declared by the installed Telethon send_file API.
        assert set(kwargs) <= set(parameters), set(kwargs) - set(parameters)
        return await observe_diagnostic_publication(lambda: self._publish(**kwargs))

    async def _publish(self, **kwargs):
        ids = [e.document_id for e in kwargs["formatting_entities"]]
        self.calls.append(ids)
        if self.uncertain:
            raise TimeoutError()
        if self.bad.intersection(ids) or (not ids and self.control_rejected):
            raise rejection()
        message_id = len(self.calls)
        message = SimpleNamespace(
            id=message_id, chat_id=CHAT if not self.wrong_identity else CHAT - 1,
            message=kwargs["caption"], entities=[
                e for e in kwargs["formatting_entities"] if e.document_id not in self.strip
            ],
        )
        self.messages[message_id] = message
        return message

    async def get_messages(self, _target, *, ids):
        msg = self.messages[ids]
        if self.wrong_text:
            return SimpleNamespace(id=ids, chat_id=CHAT, message="unrelated post", entities=[])
        return msg

    async def delete_messages(self, _target, ids, *, revoke):
        assert revoke and len(ids) == 1 and ids[0] in self.messages
        self.deleted.extend(ids)


def run_check(monkeypatch, client, caption, entities, *, chat_id=CHAT, repo=None):
    async def target(_client, _repo, _id):
        return object()

    async def generate(path):
        path.write_bytes(b"synthetic test only")

    monkeypatch.setattr(validation, "_private_test_target", target)
    monkeypatch.setattr(validation, "_generate_probe_video", generate)
    return asyncio.run(validation.VideoEmojiValidator().validate(
        client, repo or FakeRepo(), chat_id, caption, entities, user_id=7,
    ))


def test_plain_caption_does_not_need_config_or_telegram():
    result = asyncio.run(validation.VideoEmojiValidator().validate(
        None, None, "", "ordinary", [{"type": "bold", "offset": 0, "length": 8}], user_id=7,
    ))
    assert result.ok and result.status == "no_custom"


def test_utf16_range_and_untrimmed_text():
    caption = "  💎⭐️"
    occurrences, issues = validation.inspect_caption(caption, [entity(10, 2), entity(11, 4)])
    assert not issues
    assert [e.symbol for e in occurrences] == ["💎", "⭐️"]
    _, issues = validation.inspect_caption(caption, [entity(10, 3, 1)])
    assert issues[0].reason == "invalid_id_or_utf16"
    _, issues = validation.inspect_caption(caption, [entity(10, 2), entity(11, 2)])
    assert issues[0].reason == "overlapping_custom_emoji"


def test_metadata_mismatch_and_missing_doc_never_send(monkeypatch):
    client = FakeClient([document(1, "📌")])
    result = run_check(monkeypatch, client, "💎🍓", [entity(1, 0), entity(2, 2)])
    assert result.status == "invalid"
    assert {i.reason for i in result.issues} == {"emoji_text_mismatch", "document_missing"}
    assert not client.calls


def test_actual_incident_document_exists_but_send_media_rejects_both_occurrences(monkeypatch):
    ids = [1, 2, 3, 4, BAD_ID, 6, 6, BAD_ID, 1, 2]
    symbols = ["🍓", "📌", "🔓", "😈", "💎", "⭐️", "⭐️", "💎", "🍓", "📌"]
    docs = [document(i, s) for i, s in dict(zip(ids, symbols)).items()]
    client = FakeClient(docs, bad=[BAD_ID])
    caption = "".join(symbols)
    entries = [entity(i, pos * 2) for pos, i in enumerate(ids)]
    result = run_check(monkeypatch, client, caption, entries)
    assert result.status == "invalid"
    assert {(i.document_id, i.offset, i.reason) for i in result.issues} == {
        (BAD_ID, 8, "publication_rejected"), (BAD_ID, 14, "publication_rejected"),
    }
    assert len(client.calls) <= validation.MAX_PROBES
    assert set(client.deleted) == set(client.messages)
    assert [] in client.calls  # positive plain-media control


def test_valid_custom_group_verified_and_deleted(monkeypatch):
    client = FakeClient([document(1, "💎"), document(2, "⭐️")])
    result = run_check(monkeypatch, client, "💎⭐️💎", [entity(1, 0), entity(2, 2), entity(1, 4)])
    assert result.ok and result.status == "verified"
    assert client.calls == [[1, 2, 1]]
    assert client.deleted == [1]


def test_server_silently_stripping_custom_emoji_is_not_success(monkeypatch):
    client = FakeClient([document(1, "💎")], strip=[1])
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)])
    assert result.status == "invalid"
    assert result.issues[0].document_id == 1
    assert client.deleted == [1, 2]


def test_failed_plain_control_never_blames_emoji(monkeypatch):
    client = FakeClient([document(1, "💎")], bad=[1], control_rejected=True)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)])
    assert result.status == "unavailable"
    assert result.issues[0].reason == "control_failed"
    assert client.calls == [[1], []]


def test_uncertain_send_never_retries_or_deletes(monkeypatch):
    client = FakeClient([document(1, "💎")], uncertain=True)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)])
    assert result.status == "unavailable"
    assert client.calls == [[1]]
    assert not client.deleted


def test_wrong_peer_or_unrelated_text_never_deleted(monkeypatch):
    for attribute in ("wrong_identity", "wrong_text"):
        client = FakeClient([document(1, "💎")])
        setattr(client, attribute, True)
        result = run_check(monkeypatch, client, "💎", [entity(1, 0)])
        assert result.status == "unavailable"
        assert client.calls == [[1]]
        assert not client.deleted


def test_unconfigured_channel_blocks_custom_caption(monkeypatch):
    client = FakeClient([document(1, "💎")])
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], chat_id="")
    assert result.status == "unavailable" and not result.ok
    assert not client.calls


def test_registered_channel_is_rejected_before_resolving_entity():
    repo = SimpleNamespace(
        get_all_rules=lambda: [SimpleNamespace(source_id=str(CHAT), target_id="-10099")],
        get_channels=lambda _kind: [],
    )
    try:
        asyncio.run(validation._private_test_target(None, repo, CHAT))
    except RuntimeError as exc:
        assert "registered" in str(exc)
    else:
        raise AssertionError("registered production channel accepted")


def test_save_guard_and_existing_caption_button_are_wired():
    source = Path("bot.py").read_text(encoding="utf-8")
    branch = source.split('if action == "video_caption":', 1)[1].split(
        "# 2. Перенос следующего поста", 1
    )[0]
    assert branch.index("await _validate_video_caption_emoji") < branch.index("_save_video_caption_sync")
    assert 'caption = None if raw_caption.strip() in {"-", ""} else raw_caption' in branch
    assert "message.caption_entities" in branch
    assert "user_id=user_id" in branch
    assert "if not validation.ok:" in branch
    assert "if _get_user_state(user_id) is not state:" in branch
    assert 'callback_data=f"video_caption_validate:{rule_id}"' in source
    assert 'c.data.startswith("video_caption_validate:")' in source


def test_unknown_send_leaves_durable_fence_across_validator_instances(monkeypatch):
    repo = FakeRepo()
    client = FakeClient([document(1, "💎")], uncertain=True)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.status == "unavailable" and repo.inflight and repo.releases == 0
    client.uncertain = False
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.issues[0].reason == "pending_review"
    assert client.calls == [[1]]


def test_cancelled_publication_timeout_keeps_fence(monkeypatch):
    repo = FakeRepo()
    client = FakeClient([document(1, "💎")])

    async def hanging_publish(**kwargs):
        client.calls.append([e.document_id for e in kwargs["formatting_entities"]])
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "_publish", hanging_publish)
    monkeypatch.setattr(validation, "CHECK_TIMEOUT", 0.05)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.status == "unavailable"
    assert client.calls == [[1]] and repo.inflight


def test_quota_and_shared_throttle_prevent_probes(monkeypatch):
    for reason in ("quota", "flood_wait"):
        repo = FakeRepo()
        repo.reservation = reason
        client = FakeClient([document(1, "💎")])
        result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
        assert result.issues[0].reason == reason
        assert not client.calls and not repo.inflight


def test_flood_wait_persisted_and_definite_rejection_releases_fence(monkeypatch):
    repo = FakeRepo()
    client = FakeClient([document(1, "💎")])

    async def flood(**_kwargs):
        raise FloodWaitError(request=None, capture=17)

    monkeypatch.setattr(client, "_publish", flood)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.issues[0].reason == "flood_wait"
    assert repo.waits == {"mtproto:42": 18}
    assert not repo.inflight and repo.releases == 1


def test_variation_selector_difference_still_uses_real_probe(monkeypatch):
    client = FakeClient([document(1, "⭐️")])
    result = run_check(monkeypatch, client, "⭐", [entity(1, 0, 1)])
    assert result.ok and client.calls == [[1]]


def test_registered_target_channel_is_rejected():
    repo = SimpleNamespace(get_all_rules=lambda: [],
                           get_channels=lambda kind: [{"channel_id": CHAT}] if kind == "target" else [])
    assert validation._registered_chat(repo, CHAT)


def test_cancel_rule_card_resets_video_caption_edit():
    source = Path("bot.py").read_text(encoding="utf-8")
    handler = source.split("async def handle_rule_card(callback", 1)[1].split("@dp.callback_query", 1)[0]
    assert 'state.get("action") == "video_caption" and state.get("rule_id") == rule_id' in handler
    assert "reset_user_state" in handler


def test_timeout_before_publication_rpc_releases_fence(monkeypatch):
    repo = FakeRepo()
    client = FakeClient([document(1, "💎")])

    async def queued_send(**_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "send_file", queued_send)
    monkeypatch.setattr(validation, "CHECK_TIMEOUT", 0.05)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.status == "unavailable"
    assert not repo.inflight and repo.releases == 1


def test_explicit_non_emoji_rpc_rejection_releases_fence(monkeypatch):
    from telethon.errors import ChatWriteForbiddenError
    repo = FakeRepo()
    client = FakeClient([document(1, "💎")])

    async def forbidden(**_kwargs):
        raise ChatWriteForbiddenError(request=None)

    monkeypatch.setattr(client, "_publish", forbidden)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.status == "unavailable"
    assert not repo.inflight and repo.releases == 1


def test_upload_part_failure_does_not_leave_unknown_publication(monkeypatch):
    from telethon.errors import FilePartMissingError
    repo = FakeRepo()
    client = FakeClient([document(1, "💎")])

    async def upload_failure(**_kwargs):
        raise FilePartMissingError(request=functions.upload.SaveFilePartRequest(
            file_id=1, file_part=0, bytes=b"x"), capture=0)

    monkeypatch.setattr(client, "send_file", upload_failure)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.status == "unavailable"
    assert not repo.inflight and repo.releases == 1


def test_flood_after_accepted_publication_keeps_fence(monkeypatch):
    repo = FakeRepo()
    client = FakeClient([document(1, "💎")])

    async def flooded_read(*_args, **_kwargs):
        raise FloodWaitError(request=None, capture=17)

    monkeypatch.setattr(client, "get_messages", flooded_read)
    result = run_check(monkeypatch, client, "💎", [entity(1, 0)], repo=repo)
    assert result.issues[0].reason == "flood_wait"
    assert repo.inflight and not client.deleted


def test_issue_without_offset_never_displays_none():
    result = validation.ValidationResult("invalid", (validation.EmojiIssue(123, None, "invalid_id_or_utf16"),))
    text = validation.format_validation_result(result)
    assert "ID 123:" in text and "None" not in text


def _caption_input_handler(save, validate, states, *, with_clear=False):
    # Execute the actual production input-claim and save branches without
    # bootstrapping the bot, Telegram client or unrelated menu handlers.
    import ast
    import inspect
    from weakref import WeakValueDictionary

    source = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    fn = next(node for node in source.body
              if isinstance(node, ast.AsyncFunctionDef) and node.name == "handle_stateful_private_inputs")
    caption_branches = [node for node in fn.body
                        if isinstance(node, ast.If) and ast.unparse(node.test) == "action == 'video_caption'"]
    assert len(caption_branches) == 2

    async def run_db(func, *args):
        result = func(*args)
        return await result if inspect.isawaitable(result) else result

    async def noop(*_args, **_kwargs):
        pass

    ns = {
        "asyncio": asyncio, "user_states": states, "video_caption_save_locks": WeakValueDictionary(),
        "_get_user_state": lambda uid: states.get(uid) or {},
        "_is_admin_user": lambda _uid: True,
        "reset_user_state": lambda uid: states.pop(uid, None),
        "serialize_message_entities": lambda _entities: None,
        "utf16_length": validation.utf16_length, "run_db": run_db,
        "_validate_video_caption_emoji": validate, "_save_video_caption_sync": save,
        "invalidate_rule_card_cache": lambda _rule_id: None,
        "try_delete_message_safe": noop, "send_message_safe": noop,
        "reply_message_safe": noop, "format_validation_result": validation.format_validation_result,
        "build_video_caption_menu_text": lambda _rule_id: "menu",
        "build_video_caption_menu_keyboard": lambda _rule_id: None,
        "settings": SimpleNamespace(admin_id=7),
    }
    header = ast.parse(
        "async def handle(message):\n"
        "    user_id = message.from_user.id\n"
        "    state = _get_user_state(user_id)\n"
        "    if not state: return\n"
        "    action = state.get('action')\n"
    ).body[0]
    header.body.extend(caption_branches)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[header], type_ignores=[])),
                 "bot.py", "exec"), ns)
    if not with_clear:
        return ns["handle"]

    # Execute the real clear handler in the same state and lock namespace.
    clear = next(node for node in source.body
                 if isinstance(node, ast.AsyncFunctionDef) and node.name == "handle_video_caption_clear")
    clear.decorator_list = []
    clear.args.args[0].annotation = None

    async def allowed(*_args):
        return True

    async def update(rule_id, caption, entities):
        result = save(rule_id, caption, entities, 7)
        result = await result if inspect.isawaitable(result) else result
        return result["ok"]

    ns.update({
        "ensure_rule_callback_access": allowed,
        "db": SimpleNamespace(get_rule=lambda _rid: None, update_rule_video_caption=update,
                              log_rule_change=lambda **_kwargs: None),
        "answer_callback_safe": noop, "answer_callback_safe_once": noop,
        "edit_message_text_safe": noop,
    })

    async def run_db_with_kwargs(func, *args, **kwargs):
        result = func(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result

    ns["run_db"] = run_db_with_kwargs
    exec(compile(ast.fix_missing_locations(ast.Module(body=[clear], type_ignores=[])),
                 "bot.py", "exec"), ns)
    return ns["handle"], ns["handle_video_caption_clear"]


def _caption_input(mid, text):
    return SimpleNamespace(message_id=mid, text=text, caption=None, entities=[], caption_entities=[],
                           from_user=SimpleNamespace(id=7), chat=SimpleNamespace(id=7))


def test_new_input_invalidates_older_validation_and_does_not_reset_new_session():
    async def scenario():
        entered, resume = asyncio.Event(), asyncio.Event()
        writes = []
        states = {7: {"action": "video_caption", "rule_id": 141}}

        async def validate(caption, _entities, **_kwargs):
            if caption == "old":
                entered.set()
                await resume.wait()
            return validation.ValidationResult("verified")

        def save(_rule, caption, *_args):
            writes.append(caption)
            return {"ok": True}

        handle = _caption_input_handler(save, validate, states)
        old = asyncio.create_task(handle(_caption_input(100, "old")))
        await entered.wait()
        await handle(_caption_input(101, "new"))
        # Simulate opening another input while the old probe is still running.
        new_state = {"action": "video_caption", "rule_id": 142}
        states[7] = new_state
        resume.set()
        await old
        assert writes == ["new"]
        assert states[7] is new_state

    asyncio.run(scenario())


def test_inflight_db_save_cannot_finish_after_newer_input_commit():
    async def scenario():
        entered, resume, new_validated = asyncio.Event(), asyncio.Event(), asyncio.Event()
        writes = []
        states = {7: {"action": "video_caption", "rule_id": 141}}

        async def validate(caption, _entities, **_kwargs):
            if caption == "new":
                new_validated.set()
            return validation.ValidationResult("verified")

        async def save(_rule, caption, *_args):
            if caption == "old":
                entered.set()
                await resume.wait()
            writes.append(caption)
            return {"ok": True}

        handle = _caption_input_handler(save, validate, states)
        old = asyncio.create_task(handle(_caption_input(100, "old")))
        await entered.wait()
        new = asyncio.create_task(handle(_caption_input(101, "new")))
        await new_validated.wait()
        assert writes == []
        resume.set()
        await asyncio.gather(old, new)
        assert writes == ["old", "new"]
        assert 7 not in states

    asyncio.run(scenario())


@pytest.mark.parametrize("save_already_started", [False, True])
def test_clear_wins_against_pending_caption_validation_or_save(save_already_started):
    async def scenario():
        entered, resume = asyncio.Event(), asyncio.Event()
        writes = []
        other_rule = {"action": "video_caption", "rule_id": 142}
        other_flow = {"action": "something_else", "rule_id": 141}
        states = {7: {"action": "video_caption", "rule_id": 141},
                  8: {"action": "video_caption", "rule_id": 141},
                  9: other_rule, 10: other_flow}

        async def validate(*_args, **_kwargs):
            if not save_already_started:
                entered.set()
                await resume.wait()
            return validation.ValidationResult("verified")

        async def save(_rule, caption, *_args):
            if caption is not None and save_already_started:
                entered.set()
                await resume.wait()
            writes.append(caption)
            return {"ok": True}

        handle, clear = _caption_input_handler(save, validate, states, with_clear=True)
        old = asyncio.create_task(handle(_caption_input(100, "old")))
        await entered.wait()
        callback = SimpleNamespace(data="video_caption_clear:141",
                                   from_user=SimpleNamespace(id=8), message=None)
        clearing = asyncio.create_task(clear(callback))
        await asyncio.sleep(0)
        assert 7 not in states and 8 not in states
        assert states[9] is other_rule and states[10] is other_flow
        if save_already_started:
            assert writes == []
        else:
            await clearing
            assert writes == [None]
        resume.set()
        await asyncio.gather(old, clearing)
        assert writes == (["old", None] if save_already_started else [None])

    asyncio.run(scenario())
