from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from telethon import functions, types
from telethon.errors import DocumentInvalidError

from app import video_emoji_validation as validation

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

    async def __call__(self, request):
        assert isinstance(request, functions.messages.GetCustomEmojiDocumentsRequest)
        return [doc for doc in self.docs if doc.id in request.document_id]

    async def send_file(self, **kwargs):
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


def run_check(monkeypatch, client, caption, entities, *, chat_id=CHAT):
    async def target(_client, _repo, _id):
        return object()

    async def generate(path):
        path.write_bytes(b"synthetic test only")

    monkeypatch.setattr(validation, "_private_test_target", target)
    monkeypatch.setattr(validation, "_generate_probe_video", generate)
    return asyncio.run(validation.VideoEmojiValidator().validate(
        client, object(), chat_id, caption, entities,
    ))


def test_plain_caption_does_not_need_config_or_telegram():
    result = asyncio.run(validation.VideoEmojiValidator().validate(
        None, None, "", "ordinary", [{"type": "bold", "offset": 0, "length": 8}],
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
        get_channels=lambda: [],
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
    assert 'caption = None if text == "-" or not text else (message.text or "")' in branch
    assert "if not validation.ok:" in branch
    assert "if _get_user_state(user_id) is not state:" in branch
    assert 'callback_data=f"video_caption_validate:{rule_id}"' in source
    assert 'c.data.startswith("video_caption_validate:")' in source
