"""Per-delivery publication fence and durable MTProto request identity."""
from __future__ import annotations

import hashlib
import json
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge, TelegramRetryAfter,
)
from telethon import functions, types, utils
from telethon.errors import BadRequestError, ForbiddenError, FloodWaitError, SlowModeWaitError, RandomIdDuplicateError

from .runtime_utils import run_db
from .delivery_job_fence import assert_delivery_job_current
from .transport_policy import TransportRateLimited
from .video_send_diagnostics import media_request_details, record_error

logger = logging.getLogger("forwarder.transport")


class PublicationWriteUnavailable(RuntimeError):
    """Publication preflight could not establish durable identity."""


class PublicationAlreadyAttempted(RuntimeError):
    """A previous RPC may have published this delivery."""


@dataclass
class PublicationIntent:
    repository: Any
    key: str
    ordinal: int = 0
    possible_write: bool = False
    blocked: bool = False
    uncertain: bool = False
    kind: str = "delivery"
    target_thread_id: int | None = None
    video_diagnostics: dict[str, Any] | None = None


_intent: ContextVar[PublicationIntent | None] = ContextVar("publication_intent", default=None)
_single_rpc: ContextVar[bool] = ContextVar("publication_single_rpc", default=False)


@contextmanager
def publication_scope(intent: PublicationIntent):
    token = _intent.set(intent)
    try:
        yield
    finally:
        _intent.reset(token)


def current_publication_intent() -> PublicationIntent | None:
    return _intent.get()


def authorize_next_album_member() -> None:
    """Only the one-by-one loop may open a new, confirmed distinct member."""
    intent = _intent.get()
    if intent is None:
        return
    if intent.uncertain:
        raise PublicationAlreadyAttempted("Previous album member outcome is unknown")
    intent.blocked = False


def _definite_rejection(exc: Exception) -> bool:
    return not isinstance(exc, RandomIdDuplicateError) and isinstance(
        exc, (PublicationWriteUnavailable, TelegramBadRequest,
              TelegramForbiddenError, TelegramEntityTooLarge,
              TelegramRetryAfter, BadRequestError, ForbiddenError,
              FloodWaitError, SlowModeWaitError),
    )


def _check_fence(intent: PublicationIntent) -> None:
    if intent.blocked:
        raise PublicationAlreadyAttempted("Previous publication must be reconciled; no alternate send")


async def run_with_cooldown(call, *, account_scope, backend, op_name, peer_scope=None):
    intent = _intent.get()
    if intent is None or not account_scope:
        return await call()
    repo = intent.repository
    get_wait = getattr(repo, "get_transport_cooldown", None)
    extend = getattr(repo, "extend_transport_cooldown", None)
    if not callable(get_wait) or not callable(extend):
        return await call()
    try:
        scopes = [account_scope] + ([peer_scope] if peer_scope else [])
        remaining = max([await run_db(get_wait, scope) for scope in scopes])
    except Exception as exc:
        intent.blocked = True
        raise PublicationWriteUnavailable("Could not read shared Telegram cooldown") from exc
    if remaining > 0:
        raise TransportRateLimited(retry_after_seconds=remaining, backend=backend, op_name=op_name, key=account_scope)
    try:
        return await call()
    except (TelegramRetryAfter, FloodWaitError, SlowModeWaitError) as exc:
        seconds = exc.retry_after if isinstance(exc, TelegramRetryAfter) else exc.seconds
        scope = peer_scope if isinstance(exc, SlowModeWaitError) and peer_scope else account_scope
        try:
            await run_db(extend, scope, max(1, int(seconds)) + 1)
        except Exception:
            logger.exception("TRANSPORT_SHARED_COOLDOWN_PERSIST_FAILED | backend=%s", backend)
        raise


async def observe_publication(call, *, bot_result=False, expected_count=None):
    await assert_delivery_job_current()
    intent = _intent.get()
    if intent is None:
        return await call()
    _check_fence(intent)
    previous = intent.possible_write
    intent.possible_write = intent.blocked = intent.uncertain = True
    try:
        result = await call()
    except Exception as exc:
        if isinstance(exc, PublicationWriteUnavailable):
            # Preflight failure is a definite no-RPC, but MUST keep the
            # current intent fenced against alternate transport fallback.
            intent.possible_write = previous
            intent.blocked = True
            intent.uncertain = False
        elif _definite_rejection(exc):
            intent.possible_write = previous
            intent.blocked = intent.uncertain = False
        raise
    # Cancellation is deliberately not a definite rejection.
    if bot_result:
        members = result if isinstance(result, (list, tuple)) else [result]
        intent.uncertain = (
            not members
            or (expected_count is not None and len(members) != expected_count)
            or any(not isinstance(getattr(member, "message_id", None), int)
                   or member.message_id <= 0 for member in members)
        )
    else:
        intent.uncertain = result is None
    return result


def _publication_members(request):
    if isinstance(request, functions.messages.SendMultiMediaRequest):
        return list(request.multi_media)
    if isinstance(request, (functions.messages.SendMessageRequest, functions.messages.SendMediaRequest)):
        return [request]
    if isinstance(request, functions.messages.ForwardMessagesRequest):
        return list(request.id)
    return []


def _stable_descriptor(value):
    """Keep routing/text/shape; upload handles are freshly allocated on retry.

    The attempt row identifies the source delivery. This is not a content hash
    of uploaded bytes and is never used to authorize replay of an unknown send.
    """
    if isinstance(value, list):
        return [_stable_descriptor(item) for item in value]
    if isinstance(value, dict):
        return {
            key: ({"kind": item.get("_")} if key == "media" and isinstance(item, dict)
                  else _stable_descriptor(item))
            for key, item in value.items()
            if key not in {"random_id", "file_reference", "access_hash"}
        }
    return value


async def _bind_identity(account_id: int, intent: PublicationIntent, request, members) -> None:
    _check_fence(intent)
    try:
        descriptor = {"account": account_id, "request": _stable_descriptor(request.to_dict())}
        fingerprint = hashlib.sha256(
            json.dumps(descriptor, sort_keys=True, default=str).encode()
        ).hexdigest()
        ids = await run_db(
            (intent.repository.get_or_create_campaign_mtproto_random_ids
             if intent.kind == "campaign" else intent.repository.get_or_create_mtproto_random_ids),
            intent.key, intent.ordinal, fingerprint, len(members),
        )
        if (len(ids) != len(members) or len(set(ids)) != len(ids)
                or any(type(value) is not int or not 0 < value < 2**63 for value in ids)):
            raise ValueError("Invalid durable random_id vector")
        if isinstance(request, functions.messages.ForwardMessagesRequest):
            request.random_id = ids
        else:
            for member, random_id in zip(members, ids):
                member.random_id = random_id
        intent.ordinal += 1
    except Exception as exc:
        intent.blocked = True
        raise PublicationWriteUnavailable("Could not persist MTProto publication identity") from exc


def publication_client_class(base):
    """Scope Telethon 1.x internal retry limits to publication RPCs only."""
    class PublicationClient(base):
        @property
        def _request_retries(self):
            return 0 if _single_rpc.get() else getattr(self, "_normal_request_retries", 0)

        @_request_retries.setter
        def _request_retries(self, value):
            self._normal_request_retries = value

        @property
        def flood_sleep_threshold(self):
            return 0 if _single_rpc.get() else getattr(self, "_flood_sleep_threshold", 0)

        @flood_sleep_threshold.setter
        def flood_sleep_threshold(self, value):
            self._flood_sleep_threshold = min(value or 0, 24 * 60 * 60)

        async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
            invoke = super().__call__
            intent = _intent.get()
            calls = request if isinstance(request, (list, tuple)) else [request]
            publications = [(item, _publication_members(item)) for item in calls]
            publications = [(item, members) for item, members in publications if members]
            scoped = intent is not None and bool(publications)
            token = _single_rpc.set(scoped)
            try:
                if not scoped:
                    if publications:
                        await assert_delivery_job_current()
                    trace = intent.video_diagnostics if intent is not None else None
                    upload = trace is not None and type(request).__name__ in {"SaveFilePartRequest", "SaveBigFilePartRequest"}
                    if upload:
                        trace["upload_parts_started"] = trace.get("upload_parts_started", 0) + 1
                    try:
                        result = await invoke(request, ordered=ordered, flood_sleep_threshold=flood_sleep_threshold)
                    except Exception as exc:
                        if upload:
                            trace["upload_parts_failed"] = trace.get("upload_parts_failed", 0) + 1
                            record_error(trace, exc)
                        raise
                    if upload and result is True:
                        trace["upload_parts_acknowledged"] = trace.get("upload_parts_acknowledged", 0) + 1
                    return result
                _check_fence(intent)
                if getattr(self, "publication_inbox_required", False) and (
                    getattr(self, "publication_write_blocked", False)
                    or getattr(self, "publication_inbox", None) is None
                ):
                    intent.blocked = True
                    raise PublicationWriteUnavailable("MTProto publication evidence unavailable")
                if len(calls) != 1:
                    intent.blocked = True
                    raise PublicationWriteUnavailable("Batched publication RPC is not supported")
                item, members = publications[0]
                # get_me(input_peer=True) uses Telethon's cached self peer.
                # No auth key, token or password participates in the identity.
                try:
                    me = await self.get_me(input_peer=True)
                    account_id = getattr(me, "user_id", None)
                    if isinstance(me, types.InputPeerSelf):
                        # Resolve the actual User rather than recursively resolving self.
                        account_id = getattr(await self.get_me(input_peer=False), "id", None)
                    if type(account_id) is not int or account_id <= 0:
                        raise ValueError("MTProto sender account is not authorized")
                except (FloodWaitError, SlowModeWaitError):
                    raise
                except Exception as exc:
                    intent.blocked = True
                    raise PublicationWriteUnavailable("Could not resolve MTProto sender account") from exc
                read_incident = getattr(intent.repository,"get_publication_inbox_incident",None)
                if callable(read_incident):
                    try:
                        incident = await run_db(read_incident,account_id)
                    except Exception as exc:
                        intent.blocked = True
                        raise PublicationWriteUnavailable("Could not inspect durable inbox health") from exc
                    if incident and incident["acknowledged_at"] is None:
                        intent.blocked = True
                        raise PublicationWriteUnavailable("Publication inbox requires operator review")
                account_scope = f"mtproto:{account_id}"
                peer = getattr(item, "peer", None) or getattr(item, "to_peer", None)
                try:
                    peer_scope = f"{account_scope}:{utils.get_peer_id(peer)}"
                except (TypeError, ValueError):
                    peer_scope = None

                async def publish():
                    await _bind_identity(account_id, intent, item, members)
                    # The last readiness check must run immediately before
                    # invoke, after all DB/identity/cooldown awaits.
                    async def publish_fenced():
                        if getattr(self, "publication_write_blocked", False) or (
                            getattr(self, "publication_inbox_required", False)
                            and getattr(self, "publication_inbox", None) is None
                        ):
                            intent.blocked = True
                            raise PublicationWriteUnavailable("MTProto publication inbox no longer writable")
                        trace = intent.video_diagnostics
                        if trace is None or not isinstance(item, functions.messages.SendMediaRequest):
                            return await invoke(request, ordered=ordered, flood_sleep_threshold=0)
                        trace["rpc_state"] = "invoked"
                        trace["rpc_request"] = media_request_details(item)
                        try:
                            result = await invoke(request, ordered=ordered, flood_sleep_threshold=0)
                        except Exception as exc:
                            trace["rpc_state"] = "rejected" if _definite_rejection(exc) else "exception_unknown"
                            record_error(trace, exc)
                            raise
                        trace["rpc_state"] = "response_received"
                        trace["rpc_response_type"] = type(result).__name__
                        return result

                    return await observe_publication(publish_fenced)

                return await run_with_cooldown(
                    publish, account_scope=account_scope, peer_scope=peer_scope,
                    backend="telethon", op_name="publish",
                )
            finally:
                _single_rpc.reset(token)

    return PublicationClient
