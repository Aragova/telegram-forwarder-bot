"""Bounded, read-only MTProto difference recovery for exact publication evidence.

The independent PostgreSQL cursor is advanced only after storing all exact
pairs in a response. No unrelated updates are dispatched to bot handlers.
Telegram may omit a non-PTS scheduled deletion from a difference: absence
is never proof of delivery, and a missing pair must never trigger a resend.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from telethon import functions, types, utils
from telethon.errors import FloodWaitError

from .telegram_publication import PublicationInbox, publication_pairs

logger = logging.getLogger("forwarder.publication")
_RPC_TIMEOUT_SEC = 15
_RETRY_INTERVAL_SEC = 30
_MAX_PAGES = 4


class PublicationDifferenceRecovery:
    def __init__(self, client: Any, inbox: PublicationInbox):
        self.client = client
        self.inbox = inbox
        self.repository = inbox.repository
        self.sender_account_id = inbox.sender_account_id
        self._lock = asyncio.Lock()
        self._next_attempt = 0.0

    async def _rpc(self, request: Any) -> Any:
        return await asyncio.wait_for(
            self.client(request, flood_sleep_threshold=0), timeout=_RPC_TIMEOUT_SEC,
        )

    async def initialize(self) -> None:
        cursor = await asyncio.to_thread(
            self.repository.get_publication_recovery_cursor, self.sender_account_id,
        )
        if cursor is not None:
            return
        state = await self._rpc(functions.updates.GetStateRequest())
        await asyncio.to_thread(
            self.repository.initialize_publication_recovery_cursor,
            self.sender_account_id, state.pts, state.qts, state.date,
        )

    async def _advance(self, cursor: dict, pts: int, qts: int, date: Any) -> bool:
        return await asyncio.to_thread(
            self.repository.advance_publication_recovery_cursor,
            self.sender_account_id, cursor["version"], pts, qts, date,
        )

    async def _persist_pairs(self, updates: list) -> None:
        for update in updates:
            if not isinstance(update, types.UpdateDeleteScheduledMessages):
                continue
            sent = getattr(update, "sent_messages", None)
            pairs = publication_pairs(update.messages, sent)
            if not pairs:
                continue
            peer = utils.get_peer_id(update.peer)
            if not await self.inbox.record(peer, update.messages, sent):
                raise RuntimeError("Publication difference evidence rejected")

    async def _read_page(self, cursor: dict) -> bool:
        difference = await self._rpc(functions.updates.GetDifferenceRequest(
            pts=cursor["pts"], qts=cursor["qts"], date=cursor["state_date"],
            pts_limit=1000, pts_total_limit=10000,
        ))
        if isinstance(difference, types.updates.DifferenceTooLong):
            # The caller's total limit can itself cause TooLong. Retry from
            # the same checkpoint without that limit before abandoning it.
            difference = await self._rpc(functions.updates.GetDifferenceRequest(
                pts=cursor["pts"], qts=cursor["qts"], date=cursor["state_date"],
                pts_limit=1000,
            ))
        if isinstance(difference, types.updates.DifferenceTooLong):
            # This is a *read* cursor only until its page has been persisted.
            # Never persist an advanced checkpoint from GetState/TooLong:
            # doing so can erase the only chance of recovering an exact pair.
            logger.error(
                "PUBLICATION_DIFFERENCE_RANGE_UNAVAILABLE | account=%s | pts=%s | action=durable_manual_review",
                self.sender_account_id, cursor["pts"],
            )
            # This checkpoint cannot be recovered automatically. Keep the
            # original cursor, block new MTProto publication and record a
            # durable incident; pretending that another 30s retry is progress
            # would leave later mappings silently unresolved.
            # An acknowledged identical gap must not manufacture a new
            # incident or re-fence a restarted sender. Never clear an
            # existing block here; it may represent another storage failure.
            read_incident = getattr(self.repository, "get_publication_inbox_incident", None)
            if callable(read_incident):
                try:
                    incident = await asyncio.to_thread(read_incident, self.sender_account_id)
                except Exception:
                    self.client.publication_write_blocked = True
                    raise
                if (isinstance(incident, dict)
                        and incident.get("last_peer_id") == 0
                        and incident.get("last_scheduled_ids") == [int(cursor["pts"])]
                        and incident.get("acknowledged_at") is not None):
                    return False
            self.client.publication_write_blocked = True
            persist_incident = getattr(self.repository, "record_publication_inbox_overflow", None)
            if callable(persist_incident):
                await asyncio.to_thread(
                    persist_incident, self.sender_account_id, 0,
                    [int(cursor["pts"])],
                )
            return False
        if isinstance(difference, types.updates.DifferenceEmpty):
            await self._advance(cursor, cursor["pts"], cursor["qts"], difference.date)
            return False
        if isinstance(difference, types.updates.DifferenceSlice):
            state = difference.intermediate_state
            more = True
        elif isinstance(difference, types.updates.Difference):
            state = difference.state
            more = False
        else:
            raise RuntimeError("Unknown Telegram difference response")
        await self._persist_pairs(difference.other_updates)
        advanced = await self._advance(cursor, state.pts, state.qts, state.date)
        return advanced and more

    async def recover(self) -> None:
        """One bounded replay pass per account, on demand from reconciliation."""
        if self._lock.locked() or time.monotonic() < self._next_attempt:
            return
        async with self._lock:
            try:
                for _ in range(_MAX_PAGES):
                    cursor = await asyncio.to_thread(
                        self.repository.get_publication_recovery_cursor, self.sender_account_id,
                    )
                    if cursor is None or not await self._read_page(cursor):
                        break
            except FloodWaitError as exc:
                self._next_attempt = time.monotonic() + max(_RETRY_INTERVAL_SEC, exc.seconds)
                logger.warning("PUBLICATION_DIFFERENCE_FLOOD_WAIT | seconds=%s", exc.seconds)
                return
            except Exception:
                logger.exception("PUBLICATION_DIFFERENCE_RECOVERY_FAILED")
            self._next_attempt = time.monotonic() + _RETRY_INTERVAL_SEC
