"""Exact scheduled-to-published ID evidence from Telegram MTProto.

This module never sends, resends, deletes, edits or guesses messages.
The inbox stores protocol evidence independently of the delivery attempt;
an update is allowed to arrive before the send RPC returns.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("forwarder.publication")


def publication_pairs(scheduled: Any, published: Any) -> list[tuple[int, int]]:
    """Validate the entire vector pair; ordinary scheduled deletion has no mapping."""
    if not isinstance(scheduled, (tuple, list)) or not isinstance(published, (tuple, list)):
        return []
    if not scheduled or len(scheduled) != len(published):
        return []
    if any(type(mid) is not int or mid <= 0 for mid in (*scheduled, *published)):
        return []
    if len(set(scheduled)) != len(scheduled) or len(set(published)) != len(published):
        return []
    return list(zip(scheduled, published))


def publication_candidate_mask(ids: list[int], pending_mask: list[bool] | None) -> list[bool]:
    """A numeric ID can legitimately occur once in each Telegram namespace."""
    mask = [True] * len(ids) if pending_mask is None else pending_mask
    if (not ids or len(ids) != len(mask) or not any(mask)
            or any(type(mid) is not int or mid <= 0 for mid in ids)
            or any(type(flag) is not bool for flag in mask)
            or len(set(zip(mask,ids))) != len(ids)):
        return []
    return list(mask)


def verified_published_message(
    message: Any, *, peer_id: int, published_id: int, thread_id: int | None,
    source_ids: set[int] | None, before_id: int | None, started_at: datetime,
    broadcast: bool, require_scheduled: bool = True,
) -> bool:
    """Verify a protocol-mapped message in the intended peer, not by caption."""
    if message is None or getattr(message, "id", None) != published_id:
        return False
    if getattr(message, "chat_id", None) != peer_id:
        return False
    if (getattr(message, "from_scheduled", None) is True) != require_scheduled:
        return False
    if getattr(message, "video_processing_pending", False):
        return False
    if broadcast and getattr(message, "post", None) is not True:
        return False
    if not broadcast and getattr(message, "out", None) is not True:
        return False
    if before_id is not None and published_id <= before_id:
        return False
    if source_ids and published_id in source_ids:
        return False
    date = getattr(message, "date", None)
    if not isinstance(date, datetime) or date.tzinfo is None:
        return False
    if started_at.tzinfo is None or date.astimezone(timezone.utc) < started_at.astimezone(timezone.utc).replace(microsecond=0):
        return False
    if thread_id is not None:
        reply = getattr(message, "reply_to", None)
        top = getattr(reply, "reply_to_top_id", None) or getattr(reply, "top_msg_id", None)
        if top is None:
            top = getattr(reply, "reply_to_msg_id", None)
        if top != thread_id:
            return False
    return True


class PublicationInbox:
    def __init__(self, repository: Any, sender_account_id: int):
        if type(sender_account_id) is not int or sender_account_id <= 0:
            raise ValueError("Invalid sender account")
        self.repository = repository
        self.sender_account_id = sender_account_id

    async def record(self, peer_id: int, scheduled: Any, published: Any) -> bool:
        pairs = publication_pairs(scheduled, published)
        if type(peer_id) is not int or peer_id == 0 or not pairs:
            return False
        await asyncio.to_thread(
            self.repository.record_telegram_publication_mappings,
            self.sender_account_id, peer_id, pairs,
        )
        return True

    async def lookup(self, peer_id: int, scheduled_id: int) -> int | None:
        return await asyncio.to_thread(
            self.repository.get_telegram_publication_mapping,
            self.sender_account_id, peer_id, scheduled_id,
        )

    async def lookup_many(self, peer_id: int, scheduled_ids: list[int]) -> list[int]:
        return await asyncio.to_thread(
            self.repository.get_telegram_publication_mappings,
            self.sender_account_id, peer_id, scheduled_ids,
        )


def prepare_publication_inbox(client: Any, repository: Any):
    """Install before connect; only bind events after authenticating the account."""
    from collections import deque
    from telethon import events, types, utils

    buffer = deque(maxlen=1024)
    inbox: PublicationInbox | None = None

    async def handle(update: Any) -> None:
        if not isinstance(update, types.UpdateDeleteScheduledMessages):
            return
        try:
            peer = utils.get_peer_id(update.peer)
            pairs = publication_pairs(update.messages,getattr(update,"sent_messages",None))
            if not pairs:
                return
            old,new = map(list,zip(*pairs))
            if inbox is None:
                if len(buffer)==buffer.maxlen:
                    logger.error("TELEGRAM_PUBLICATION_STARTUP_BUFFER_FULL")
                buffer.append((peer,old,new))
                return
            await inbox.record(peer,old,new)
        except Exception:
            logger.exception("TELEGRAM_PUBLICATION_MAPPING_PERSIST_FAILED")

    client.add_event_handler(handle,events.Raw(types=types.UpdateDeleteScheduledMessages))

    async def bind() -> PublicationInbox:
        nonlocal inbox
        me = await asyncio.wait_for(client.get_me(),timeout=30)
        if me is None:
            raise RuntimeError("Publication inbox requires an authenticated sender")
        inbox = PublicationInbox(repository,int(me.id))
        while buffer:
            peer,old,new = buffer.popleft()
            await inbox.record(peer,old,new)
        client.publication_inbox = inbox
        return inbox

    return bind


async def install_publication_inbox(client: Any, repository: Any) -> PublicationInbox:
    """Compatibility for isolated clients; runtime installs before connect."""
    bind = prepare_publication_inbox(client,repository)
    return await bind()


def botapi_publication_ids(messages: Any, expected_count: int) -> list[int]:
    """A successful Bot API response may be scheduled (ID=0) or partial.

    Return only a complete, positive, unique ID vector. An empty vector here
    never means that the request was rejected or can safely be sent again.
    """
    items = messages if isinstance(messages, (list, tuple)) else [messages]
    ids = [getattr(item,"message_id",None) for item in items]
    if len(ids) != expected_count or not publication_pairs(ids,ids):
        return []
    return ids
