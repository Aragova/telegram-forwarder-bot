"""Validate Premium video captions without publishing customer media.

A document lookup is necessary but insufficient: Telegram may reject an existing
Custom Emoji in SendMedia. Only explicitly requested UI checks may send a
synthetic clip to an administrator-configured, isolated private channel.
"""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from telethon import functions, types, utils
from telethon.errors import DocumentInvalidError, FloodWaitError

from .runtime_utils import run_db
from .send_intent import current_publication_intent
from .subprocess_safety import communicate_safely
from .transport_policy import TransportRateLimited

logger = logging.getLogger("forwarder")
MAX_OCCURRENCES = 50
MAX_PROBES = 12
CHECK_TIMEOUT = 60
MIN_CHECK_INTERVAL = 3


@dataclass(frozen=True, slots=True)
class EmojiOccurrence:
    document_id: int
    offset: int
    length: int
    symbol: str


@dataclass(frozen=True, slots=True)
class EmojiIssue:
    document_id: int | None
    offset: int | None
    reason: str


@dataclass(frozen=True, slots=True)
class ValidationResult:
    status: str
    issues: tuple[EmojiIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return self.status in {"no_custom", "verified"}


def utf16_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def inspect_caption(caption, raw_entities):
    """Preserve original UTF-16 offsets; never normalize or mutate the caption."""
    occurrences = []
    issues = []
    if not raw_entities:
        return occurrences, issues
    try:
        parsed = json.loads(raw_entities) if isinstance(raw_entities, str) else raw_entities
        if isinstance(parsed, dict):
            parsed = [parsed]
        if not isinstance(parsed, list):
            raise ValueError("invalid entities")
        encoded = (caption or "").encode("utf-16-le")
    except (ValueError, TypeError, UnicodeError):
        return [], [EmojiIssue(None, None, "invalid_entities")]

    for item in parsed:
        if not isinstance(item, dict):
            issues.append(EmojiIssue(None, None, "invalid_entities"))
            continue
        if str(item.get("type") or "").strip().lower() != "custom_emoji":
            continue
        document_id = None
        offset = None
        try:
            raw_id = item["custom_emoji_id"]
            if isinstance(raw_id, bool) or not isinstance(raw_id, (str, int)):
                raise ValueError("invalid document id")
            document_id = int(raw_id)
            if not 0 < document_id < 2**63:
                raise ValueError("invalid document id")
            raw_offset, raw_length = item["offset"], item["length"]
            if any(isinstance(v, bool) or not isinstance(v, (str, int)) for v in (raw_offset, raw_length)):
                raise ValueError("invalid range")
            offset, length = int(raw_offset), int(raw_length)
            if offset < 0 or length <= 0 or 2 * (offset + length) > len(encoded):
                raise ValueError("invalid range")
            symbol = encoded[2 * offset:2 * (offset + length)].decode("utf-16-le")
        except (KeyError, TypeError, ValueError, UnicodeError):
            issues.append(EmojiIssue(document_id, offset if type(offset) is int else None,
                                     "invalid_id_or_utf16"))
            continue
        occurrences.append(EmojiOccurrence(document_id, offset, length, symbol))

    ordered = sorted(occurrences, key=lambda item: item.offset)
    for previous, current in zip(ordered, ordered[1:]):
        if current.offset < previous.offset + previous.length:
            issues.append(EmojiIssue(current.document_id, current.offset, "overlapping_custom_emoji"))
    if len(occurrences) > MAX_OCCURRENCES:
        issues.append(EmojiIssue(None, None, "too_many_custom_emoji"))
    return occurrences, issues


async def check_documents(client, occurrences):
    ids = list(dict.fromkeys(item.document_id for item in occurrences))
    documents = await client(functions.messages.GetCustomEmojiDocumentsRequest(document_id=ids))
    attributes = {}
    for doc in documents:
        if isinstance(doc, types.Document):
            attr = next((a for a in doc.attributes
                         if isinstance(a, types.DocumentAttributeCustomEmoji)), None)
            if attr is not None:
                attributes[doc.id] = attr
    issues = []
    for item in occurrences:
        attr = attributes.get(item.document_id)
        if attr is None:
            issues.append(EmojiIssue(item.document_id, item.offset, "document_missing"))
        elif attr.alt != item.symbol:
            issues.append(EmojiIssue(item.document_id, item.offset, "emoji_text_mismatch"))
    return issues


def _registered_chat(repo, chat_id):
    for rule in repo.get_all_rules():
        if str(chat_id) in (str(rule.source_id), str(rule.target_id)):
            return True
    return any(str(row["channel_id"]) == str(chat_id) for row in repo.get_channels())


async def _private_test_target(client, repo, chat_id):
    if await run_db(_registered_chat, repo, chat_id):
        raise RuntimeError("validation channel is registered for forwarding")
    target = await client.get_entity(chat_id)
    if (utils.get_peer_id(target) != chat_id or not getattr(target, "broadcast", False)
            or getattr(target, "megagroup", False) or getattr(target, "username", None)
            or getattr(target, "has_link", False)
            or any(getattr(item, "active", False) for item in (getattr(target, "usernames", None) or []))):
        raise RuntimeError("validation channel must be private and isolated")
    return target


async def _generate_probe_video(path):
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-hide_banner", "-v", "error",
        "-f", "lavfi", "-i", "color=c=black:s=64x64:r=30",
        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
        "-t", "1", "-c:v", "libx264", "-preset", "ultrafast",
        "-pix_fmt", "yuv420p", "-c:a", "aac", "-threads", "1",
        "-movflags", "+faststart", "-y", str(path),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
    )
    await communicate_safely(process, timeout_seconds=10)
    if process.returncode != 0 or not path.is_file() or not path.stat().st_size:
        raise RuntimeError("synthetic video generation failed")


def _synthetic_caption(occurrences, marker):
    text = marker
    entities = []
    for item in occurrences:
        if text:
            text += " "
        entities.append(types.MessageEntityCustomEmoji(
            offset=utf16_length(text), length=utf16_length(item.symbol),
            document_id=item.document_id,
        ))
        text += item.symbol
    return text, entities


def _emoji_counter(entities):
    return Counter((item.offset, item.length, item.document_id)
                   for item in (entities or [])
                   if isinstance(item, types.MessageEntityCustomEmoji))


class ProbeBudgetExceeded(RuntimeError):
    pass


class _ProbeSession:
    def __init__(self, client, target, chat_id, path):
        self.client = client
        self.target = target
        self.chat_id = chat_id
        self.path = path
        self.count = 0

    async def accepts(self, occurrences):
        if self.count >= MAX_PROBES:
            raise ProbeBudgetExceeded()
        self.count += 1
        marker = f"ViMi emoji check {secrets.token_hex(8)}"
        caption, entities = _synthetic_caption(occurrences, marker)
        try:
            sent = await self.client.send_file(
                entity=self.target, file=str(self.path), caption=caption,
                parse_mode=None, formatting_entities=entities,
                supports_streaming=True, link_preview=False, silent=True,
                attributes=[types.DocumentAttributeVideo(
                    duration=1, w=64, h=64, supports_streaming=True, round_message=False,
                )],
            )
        except DocumentInvalidError as exc:
            # A rejection in some other RPC is not evidence against an emoji.
            if not isinstance(getattr(exc, "request", None), functions.messages.SendMediaRequest):
                raise
            return False
        message_id = getattr(sent, "id", None)
        if type(message_id) is not int or message_id <= 0 or getattr(sent, "chat_id", None) != self.chat_id:
            # No deletion or further probe when the response is ambiguous.
            raise RuntimeError("unverified probe message identity")
        observed = await self.client.get_messages(self.target, ids=message_id)
        if (getattr(observed, "id", None) != message_id
                or getattr(observed, "chat_id", None) != self.chat_id
                or getattr(observed, "message", None) != caption):
            # The unique synthetic text is part of the deletion proof.
            raise RuntimeError("probe message not verified")
        accepted = _emoji_counter(observed.entities) == _emoji_counter(entities)
        await self.client.delete_messages(self.target, [message_id], revoke=True)
        return accepted

    async def isolate(self, occurrences):
        if len(occurrences) == 1:
            item = occurrences[0]
            return [EmojiIssue(item.document_id, item.offset, "publication_rejected")]
        half = len(occurrences) // 2
        failures = []
        for subset in (occurrences[:half], occurrences[half:]):
            if not await self.accepts(subset):
                failures.extend(await self.isolate(subset))
        # If both halves pass, the rejection only proves a combination failure.
        return failures or [EmojiIssue(None, None, "combination_rejected")]


class VideoEmojiValidator:
    """One bounded check at a time, with no production publication/retry."""
    def __init__(self):
        self._lock = asyncio.Lock()
        self._next_check = 0.0

    async def validate(self, client, repo, chat_id, caption, raw_entities):
        occurrences, issues = inspect_caption(caption, raw_entities)
        if issues:
            return ValidationResult("invalid", tuple(issues))
        if not occurrences:
            return ValidationResult("no_custom")
        if current_publication_intent() is not None:
            return ValidationResult("unavailable", (EmojiIssue(None, None, "publication_scope"),))
        try:
            chat_id = int(chat_id)
            if not str(chat_id).startswith("-100") or client is None:
                raise ValueError("not configured")
        except (ValueError, TypeError):
            return ValidationResult("unavailable", (EmojiIssue(None, None, "test_channel_not_configured"),))

        if self._lock.locked() or time.monotonic() < self._next_check:
            return ValidationResult("unavailable", (EmojiIssue(None, None, "busy"),))
        async with self._lock:
            try:
                return await asyncio.wait_for(
                    self._validate_live(client, repo, chat_id, occurrences), timeout=CHECK_TIMEOUT,
                )
            except (FloodWaitError, TransportRateLimited) as exc:
                seconds = exc.seconds if isinstance(exc, FloodWaitError) else exc.retry_after_seconds
                self._next_check = max(self._next_check, time.monotonic() + max(1, seconds))
                logger.warning("VIDEO_EMOJI_VALIDATION_FLOOD_WAIT | seconds=%s", seconds)
                return ValidationResult("unavailable", (EmojiIssue(None, None, "flood_wait"),))
            except Exception as exc:
                logger.warning("VIDEO_EMOJI_VALIDATION_UNAVAILABLE | error_type=%s", type(exc).__name__)
                return ValidationResult("unavailable", (EmojiIssue(None, None, type(exc).__name__),))
            finally:
                self._next_check = max(self._next_check, time.monotonic() + MIN_CHECK_INTERVAL)

    @staticmethod
    async def _validate_live(client, repo, chat_id, occurrences):
        issues = await check_documents(client, occurrences)
        if issues:
            return ValidationResult("invalid", tuple(issues))
        target = await _private_test_target(client, repo, chat_id)
        with tempfile.TemporaryDirectory(prefix="vimi-emoji-probe-") as directory:
            path = Path(directory) / "probe.mp4"
            await _generate_probe_video(path)
            session = _ProbeSession(client, target, chat_id, path)
            if await session.accepts(occurrences):
                return ValidationResult("verified")
            # Do not attribute a rejection to an emoji unless the same synthetic
            # media can be published without the emoji.
            if not await session.accepts([]):
                return ValidationResult("unavailable", (EmojiIssue(None, None, "control_failed"),))
            issues = await session.isolate(occurrences)
            logger.warning("VIDEO_EMOJI_VALIDATION_REJECTED | ids=%s | reasons=%s",
                           sorted({i.document_id for i in issues if i.document_id is not None}),
                           sorted({i.reason for i in issues}))
            return ValidationResult("invalid", tuple(issues))


def format_validation_result(result):
    if result.status == "no_custom":
        return "✅ В подписи нет Premium-эмодзи."
    if result.status == "verified":
        return "✅ Premium-эмодзи прошли проверку публикацией в тестовом канале."
    if result.status == "unavailable":
        reason = result.issues[0].reason if result.issues else "unknown"
        descriptions = {
            "test_channel_not_configured": "не настроен приватный канал проверки",
            "busy": "проверка занята, повторите немного позже",
            "flood_wait": "Telegram ограничил частоту запросов",
            "control_failed": "не прошла контрольная публикация без эмодзи",
            "ProbeBudgetExceeded": "исчерпан лимит диагностических публикаций",
        }
        return "⚠️ Не удалось подтвердить Premium-эмодзи: " + descriptions.get(reason, "проверка временно недоступна") + "."
    descriptions = {
        "publication_rejected": "Telegram отклонил при публикации",
        "document_missing": "документ не найден",
        "emoji_text_mismatch": "не совпадает с обычным эмодзи",
        "invalid_id_or_utf16": "некорректный ID или позиция",
        "combination_rejected": "Telegram отклонил комбинацию эмодзи",
        "overlapping_custom_emoji": "перекрывающиеся позиции",
        "too_many_custom_emoji": "слишком много Premium-эмодзи",
        "invalid_entities": "некорректное форматирование",
    }
    lines = ["❌ Проверка Premium-эмодзи не пройдена."]
    for issue in result.issues[:10]:
        prefix = f"ID {issue.document_id}, позиция {issue.offset}: " if issue.document_id else ""
        lines.append(prefix + descriptions.get(issue.reason, "некорректный эмодзи"))
    if len(result.issues) > 10:
        lines.append(f"Ещё замечаний: {len(result.issues) - 10}")
    lines.append("Замените проблемные эмодзи и повторите проверку.")
    return "\n".join(lines)
