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
_STARTUP_DRAIN_TIMEOUT_SEC = 30


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
    # Only mapped members require explicit scheduled provenance.
    if require_scheduled and getattr(message, "from_scheduled", None) is not True:
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
        # General-topic replies may target an ordinary message, without
        # a top-topic ID. Never mistake reply_to_msg_id for a topic ID.
        if thread_id != 1 and top is None:
            top = getattr(reply, "reply_to_msg_id", None)
        expected_top = None if thread_id == 1 else thread_id
        if top != expected_top:
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
    from telethon.errors import FloodWaitError, ServerError

    client.publication_inbox_ready = asyncio.get_running_loop().create_future()

    from pathlib import Path
    from .publication_mapping_spool import PublicationMappingSpool, PublicationFallbackJournal

    # Persist beside the Telethon SQLiteSession, which survives service restarts.
    session_file = getattr(getattr(client, "session", None), "filename", None)
    spool = (
        PublicationMappingSpool(Path(session_file).with_suffix(".publication-mappings.sqlite"))
        if session_file else None
    )
    # On a SQLite failure, preserve evidence in a separate fsynced journal
    # before attempting PostgreSQL. Replay it during the next bind/restart.
    journal = (
        PublicationFallbackJournal(
            Path(session_file).with_suffix(".publication-mappings.fallback.jsonl")
        ) if session_file else None
    )
    journal_lock = asyncio.Lock()
    client.publication_evidence_unpersisted = False
    # Test/injected clients without a session use an in-memory buffer only.
    buffer = deque()
    emergency = deque()
    inbox: PublicationInbox | None = None
    candidate: PublicationInbox | None = None
    buffer_overflow = False
    overflow_evidence = None
    incident_persisted = False
    live_lock = asyncio.Lock()
    spool_generation = 0
    retry_task: asyncio.Task | None = None

    async def flush_live() -> None:
        nonlocal incident_persisted
        async with live_lock:
            if candidate is None:
                return
            while True:
                spool_failure: Exception | None = None
                spool_io_failed = False
                if spool is not None:
                    try:
                        # ACK SQLite only after PostgreSQL commits the exact pair.
                        # Replaying a committed-but-unacked pair is idempotent.
                        while True:
                            # A handler can append while the empty SQLite read is
                            # running. Check a generation captured before that await
                            # so the final drain cannot discard its wakeup.
                            generation_before_read = spool_generation
                            try:
                                entries = await asyncio.to_thread(spool.pending, 128)
                            except Exception:
                                spool_io_failed = True
                                raise
                            if not entries:
                                if spool_generation != generation_before_read:
                                    continue
                                break
                            for row_id, peer, old, new in entries:
                                await candidate.record(peer, old, new)
                                try:
                                    await asyncio.to_thread(spool.acknowledge, row_id)
                                except Exception:
                                    spool_io_failed = True
                                    raise
                        # All durable entries already include the in-memory buffer.
                        buffer.clear()
                    except Exception as exc:
                        spool_failure = exc
                        # An ordinary transient PostgreSQL error is retryable:
                        # evidence is still durable in SQLite and must not force
                        # a permanent publication-write lock. Only SQLite I/O
                        # failures compromise the local evidence channel.
                        if spool_io_failed:
                            client.publication_write_blocked = True
                        logger.exception("TELEGRAM_PUBLICATION_MAPPING_DRAIN_FAILED")
                generation_after_spool = spool_generation
                # Every SQLite append failure was also written to a fsynced
                # fallback journal. Replay it under the append lock, including
                # records left by a previous process after a crash/shutdown.
                # PostgreSQL inserts are idempotent, so duplicate emergency
                # memory entries are safe to replay.
                if journal is not None:
                    async with journal_lock:
                        journal_rows = await asyncio.to_thread(journal.pending)
                        for peer, old, new in journal_rows:
                            await candidate.record(peer, old, new)
                        if journal_rows:
                            # A journal from the previous process proves
                            # SQLite became unavailable. Keep sending fenced
                            # until an operator acknowledges the persisted
                            # incident, even after PostgreSQL replays records.
                            client.publication_write_blocked = True
                            persist_incident = getattr(
                                repository, "record_publication_inbox_overflow", None
                            )
                            if callable(persist_incident):
                                first_peer, first_old, _ = journal_rows[0]
                                await asyncio.to_thread(
                                    persist_incident,
                                    candidate.sender_account_id, first_peer, first_old,
                                )
                                incident_persisted = True
                            await asyncio.to_thread(journal.clear)
                            # These in-memory duplicates were already
                            # acknowledged in PostgreSQL by the journal pass.
                            for row in journal_rows:
                                try:
                                    emergency.remove(row)
                                except ValueError:
                                    pass
                        while emergency:
                            peer, old, new = emergency[0]
                            await candidate.record(peer, old, new)
                            emergency.popleft()
                        client.publication_evidence_unpersisted = False
                else:
                    while emergency:
                        peer, old, new = emergency[0]
                        await candidate.record(peer, old, new)
                        emergency.popleft()
                if spool_failure is not None:
                    raise spool_failure
                if spool is None:
                    while buffer:
                        peer, old, new = buffer[0]
                        await candidate.record(peer, old, new)
                        buffer.popleft()
                # An append that completed while emergency persistence was
                # awaiting PostgreSQL must not be stranded in SQLite.
                # There are no awaits between this generation check and
                # releasing live_lock when it is stable.
                if spool is None or spool_generation == generation_after_spool:
                    break

    def schedule_retry(delay: float) -> None:
        nonlocal retry_task
        # Coalesce every emergency wakeup into one retry chain. Launching a
        # task per incoming update during an outage is unbounded.
        if retry_task is not None and not retry_task.done():
            return
        from .background_tasks import start_background_task
        retry_task = start_background_task(
            retry_live(delay), name="telegram-publication-persist-retry",
        )

    async def retry_live(delay: float) -> None:
        while True:
            await asyncio.sleep(delay)
            try:
                await flush_live()
                return
            except Exception:
                logger.exception("TELEGRAM_PUBLICATION_MAPPING_PERSIST_RETRY_FAILED")
                delay = min(delay * 2, 30)

    async def handle(update: Any) -> None:
        nonlocal buffer_overflow, overflow_evidence, spool_generation
        if not isinstance(update, types.UpdateDeleteScheduledMessages):
            return
        try:
            peer = utils.get_peer_id(update.peer)
            pairs = publication_pairs(update.messages,getattr(update,"sent_messages",None))
            if not pairs:
                return
            old,new = map(list,zip(*pairs))
            if spool is not None:
                try:
                    await asyncio.to_thread(spool.append, peer, old, new)
                    spool_generation += 1
                except Exception:
                    # Disk failure is not an excuse to reopen Telegram writes.
                    client.publication_write_blocked = True
                    buffer_overflow = True
                    overflow_evidence = (peer, old)
                    if journal is not None:
                        async with journal_lock:
                            try:
                                await asyncio.to_thread(journal.append, peer, old, new)
                            except Exception:
                                client.publication_evidence_unpersisted = True
                                logger.critical(
                                    "PUBLICATION_EVIDENCE_VOLATILE | sqlite_and_fallback_write_failed | action=block_shutdown",
                                    exc_info=True,
                                )
                            emergency.append((peer, old, new))
                    else:
                        client.publication_evidence_unpersisted = True
                        emergency.append((peer, old, new))
                    logger.exception("TELEGRAM_PUBLICATION_MAPPING_LOCAL_SPOOL_FAILED")
                    # Preserve evidence in memory and attempt PostgreSQL
                    # immediately. A disk failure must not strand the only
                    # remaining evidence until another Telegram update.
                    if candidate is not None:
                        if live_lock.locked() or (retry_task is not None and not retry_task.done()):
                            schedule_retry(0.25)
                        else:
                            try:
                                await flush_live()
                            except Exception:
                                logger.exception("TELEGRAM_PUBLICATION_MAPPING_EMERGENCY_PERSIST_FAILED")
                                schedule_retry(0.25)
                    return
            if len(buffer) >= 1024 and spool is None:
                # No durable spool: overflow is a real evidence-loss risk and
                # requires an operator incident. Retain the vector even here.
                buffer_overflow = True
                client.publication_write_blocked = True
                overflow_evidence = (peer, old)
                logger.error("TELEGRAM_PUBLICATION_MAPPING_BUFFER_FULL | bound=%s | peer=%s | scheduled_ids=%s", inbox is not None, peer, old)
                if candidate is not None:
                    persist = getattr(repository, "record_publication_inbox_overflow", None)
                    if callable(persist):
                        await asyncio.to_thread(persist, candidate.sender_account_id, peer, old)
                buffer.append((peer, old, new))
                return
            # When SQLite is healthy the mapping is durable already. Bound
            # the in-memory wakeup cache, but do not manufacture an incident
            # or lock MTProto writes merely because PostgreSQL is slow.
            if len(buffer) < 1024:
                buffer.append((peer, old, new))
            if inbox is None or live_lock.locked() or (retry_task is not None and not retry_task.done()):
                return
            try:
                await flush_live()
            except Exception:
                logger.exception("TELEGRAM_PUBLICATION_MAPPING_PERSIST_FAILED")
                schedule_retry(0.25)
        except Exception:
            logger.exception("TELEGRAM_PUBLICATION_MAPPING_HANDLE_FAILED")

    client.flush_publication_inbox = flush_live
    client.add_event_handler(handle,events.Raw(types=types.UpdateDeleteScheduledMessages))

    async def bind() -> PublicationInbox:
        nonlocal inbox, candidate
        from .background_tasks import start_background_task

        recovery = None
        drained = asyncio.Event()

        def fail_startup(error: Exception) -> None:
            if not client.publication_inbox_ready.done():
                client.publication_inbox_ready.set_exception(error)
                # Mark observed even if the runtime has not attached a callback.
                client.publication_inbox_ready.exception()
            drained.set()

        async def retry_startup(delay: float, *, flood_wait: float = 0) -> None:
            # Honor Telegram's delay separately from database retry backoff.
            await asyncio.sleep(max(delay, flood_wait))
            await drain_startup(min(delay * 2, 1))

        async def drain_startup(delay: float = 0.25) -> None:
            nonlocal inbox, candidate, recovery
            if candidate is None:
                try:
                    me = await asyncio.wait_for(client.get_me(), timeout=30)
                    if me is None:
                        raise RuntimeError("Publication inbox requires an authenticated sender")
                    candidate = PublicationInbox(repository, int(me.id))
                    if callable(getattr(repository, "get_publication_recovery_cursor", None)):
                        from .publication_difference_recovery import PublicationDifferenceRecovery

                        recovery = PublicationDifferenceRecovery(client, candidate)
                except FloodWaitError as exc:
                    start_background_task(
                        retry_startup(delay, flood_wait=exc.seconds),
                        name="telegram-publication-identity-retry",
                    )
                    return
                except (TimeoutError, OSError, ServerError):
                    logger.exception("TELEGRAM_PUBLICATION_IDENTITY_RETRY")
                    start_background_task(
                        retry_startup(delay), name="telegram-publication-identity-retry",
                    )
                    return
                except Exception as exc:
                    fail_startup(exc)
                    return
            try:
                await flush_live()
                read_incident = getattr(repository,"get_publication_inbox_incident",None)
                persist_incident = getattr(repository,"record_publication_inbox_overflow",None)
                if (buffer_overflow and not incident_persisted
                        and overflow_evidence is not None and callable(persist_incident)):
                    await asyncio.to_thread(persist_incident,candidate.sender_account_id,*overflow_evidence)
                if callable(read_incident):
                    incident = await asyncio.to_thread(read_incident,candidate.sender_account_id)
                    if incident and incident["acknowledged_at"] is None:
                        client.publication_write_blocked = True
                        logger.error("PUBLICATION_INBOX_REVIEW_REQUIRED | account=%s",candidate.sender_account_id)
                if recovery is not None and not hasattr(client, "publication_recovery"):
                    # Checkpoint setup is part of startup, including retry
                    # after a DB outage. Events arriving during it stay buffered.
                    await recovery.initialize()
                    client.publication_recovery = recovery
            except FloodWaitError as exc:
                logger.warning("TELEGRAM_PUBLICATION_STARTUP_FLOOD_WAIT | seconds=%s", exc.seconds)
                start_background_task(
                    retry_startup(delay, flood_wait=exc.seconds),
                    name="telegram-publication-startup-retry",
                )
                return
            except Exception:
                logger.exception("TELEGRAM_PUBLICATION_STARTUP_PERSIST_FAILED")
                start_background_task(
                    retry_startup(delay), name="telegram-publication-startup-retry",
                )
                return
            if buffer or emergency:
                # A live event arrived during checkpoint initialization.
                await drain_startup()
                return
            # Continue recording evidence for prior sends even if startup lost
            # events. Do not expose the write-readiness gate in that case.
            inbox = candidate
            if buffer_overflow:
                fail_startup(RuntimeError("Publication startup buffer overflowed"))
                return
            client.publication_inbox = candidate
            if not client.publication_inbox_ready.done():
                client.publication_inbox_ready.set_result(candidate)
            drained.set()

        # Wait for the completion signal, not the retry task itself. A startup
        # timeout must not cancel pending writes or enable MTProto sending.
        start_background_task(
            drain_startup(), name="telegram-publication-startup-drain",
        )
        await asyncio.wait_for(drained.wait(), timeout=_STARTUP_DRAIN_TIMEOUT_SEC)
        return client.publication_inbox_ready.result()

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
