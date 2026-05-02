from __future__ import annotations

import logging
from pathlib import Path

from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError

logger = logging.getLogger("forwarder")


class TenantReactionExecutor:
    def __init__(self, api_id: int, api_hash: str, base_dir: Path | str):
        self.api_id = api_id
        self.api_hash = api_hash
        self.base_dir = Path(base_dir)

    def _is_valid_session_name(self, session_name: str) -> bool:
        value = str(session_name or "").strip()
        if not value:
            return False
        if "/" in value or "\\" in value or ".." in value:
            return False
        return True

    async def _disconnect_safely(self, client, *, tenant_id, account_id, rule_id, stage):
        try:
            await client.disconnect()
        except Exception as exc:
            logger.warning(
                "TENANT_REACTION_DISCONNECT_SUPPRESSED | tenant_id=%s | account_id=%s | rule_id=%s | stage=%s | error_type=%s",
                tenant_id,
                account_id,
                rule_id,
                stage,
                exc.__class__.__name__,
            )

    async def add_reactions(
        self,
        *,
        tenant_id: int,
        accounts: list[dict],
        target_id: str | int,
        message_id: int,
        rule_id: int | None = None,
    ) -> dict:
        logger.info(
            "TENANT_REACTION_START | tenant_id=%s | rule_id=%s | target_id=%s | message_id=%s | accounts=%s",
            tenant_id,
            rule_id,
            target_id,
            message_id,
            len(accounts or []),
        )

        result = {"ok": True, "attempted": 0, "confirmed": 0, "failed": 0, "skipped": 0, "results": []}
        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id

        for account in accounts or []:
            account_id = account.get("id")
            session_name = str(account.get("session_name") or "").strip()
            fixed_reactions = account.get("fixed_reactions_json") or []
            if not isinstance(fixed_reactions, list):
                fixed_reactions = []

            if not fixed_reactions:
                result["skipped"] += 1
                result["results"].append({"account_id": account_id, "status": "skipped", "reason": "no_fixed_reactions"})
                logger.info(
                    "TENANT_REACTION_ACCOUNT_SKIPPED | tenant_id=%s | account_id=%s | rule_id=%s | reason=%s",
                    tenant_id,
                    account_id,
                    rule_id,
                    "no_fixed_reactions",
                )
                continue

            if not self._is_valid_session_name(session_name):
                result["skipped"] += 1
                result["results"].append({"account_id": account_id, "status": "skipped", "reason": "invalid_session_name"})
                logger.info(
                    "TENANT_REACTION_ACCOUNT_SKIPPED | tenant_id=%s | account_id=%s | rule_id=%s | reason=%s",
                    tenant_id,
                    account_id,
                    rule_id,
                    "invalid_session_name",
                )
                continue

            session_dir = self.base_dir / "sessions" / "tenants" / str(tenant_id) / "reactors"
            session_file = session_dir / f"{session_name}.session"
            if not session_file.exists():
                result["skipped"] += 1
                result["results"].append({"account_id": account_id, "status": "skipped", "reason": "session_file_missing"})
                logger.info(
                    "TENANT_REACTION_ACCOUNT_SKIPPED | tenant_id=%s | account_id=%s | rule_id=%s | reason=%s",
                    tenant_id,
                    account_id,
                    rule_id,
                    "session_file_missing",
                )
                continue

            result["attempted"] += 1
            session_base = session_file.with_suffix("")
            client = TelegramClient(str(session_base), self.api_id, self.api_hash)

            try:
                await client.connect()
                if not await client.is_user_authorized():
                    result["failed"] += 1
                    result["ok"] = False
                    result["results"].append({"account_id": account_id, "status": "failed", "reason": "not_authorized"})
                    logger.warning(
                        "TENANT_REACTION_ACCOUNT_FAILED | tenant_id=%s | account_id=%s | rule_id=%s | reason=%s",
                        tenant_id,
                        account_id,
                        rule_id,
                        "not_authorized",
                    )
                    continue

                reaction_payload = [types.ReactionEmoji(emoticon=str(emoji)) for emoji in fixed_reactions]
                await client(
                    functions.messages.SendReactionRequest(
                        peer=entity,
                        msg_id=message_id,
                        reaction=reaction_payload,
                        big=False,
                        add_to_recent=False,
                    )
                )
                result["confirmed"] += 1
                result["results"].append({"account_id": account_id, "status": "accepted", "reactions": fixed_reactions})
                logger.info(
                    "TENANT_REACTION_ACCEPTED | tenant_id=%s | account_id=%s | rule_id=%s | target_id=%s | message_id=%s | reactions=%s",
                    tenant_id,
                    account_id,
                    rule_id,
                    entity,
                    message_id,
                    fixed_reactions,
                )
            except FloodWaitError:
                result["failed"] += 1
                result["ok"] = False
                result["results"].append({"account_id": account_id, "status": "failed", "reason": "flood_wait"})
                logger.warning(
                    "TENANT_REACTION_STOP_ON_FLOOD_WAIT | tenant_id=%s | account_id=%s | rule_id=%s",
                    tenant_id,
                    account_id,
                    rule_id,
                )
            except Exception as exc:
                err_text = str(exc or "").lower()
                error_type = exc.__class__.__name__
                if "flood wait" in err_text:
                    logger.warning(
                        "TENANT_REACTION_STOP_ON_FLOOD_WAIT | tenant_id=%s | account_id=%s | rule_id=%s",
                        tenant_id,
                        account_id,
                        rule_id,
                    )
                else:
                    logger.warning(
                        "TENANT_REACTION_FAILED | tenant_id=%s | account_id=%s | rule_id=%s | error_type=%s",
                        tenant_id,
                        account_id,
                        rule_id,
                        error_type,
                    )
                result["failed"] += 1
                result["ok"] = False
                result["results"].append({"account_id": account_id, "status": "failed", "error_type": error_type})
            finally:
                await self._disconnect_safely(
                    client,
                    tenant_id=tenant_id,
                    account_id=account_id,
                    rule_id=rule_id,
                    stage="add_reactions",
                )

        logger.info(
            "TENANT_REACTION_DONE | tenant_id=%s | rule_id=%s | attempted=%s | confirmed=%s | failed=%s | skipped=%s",
            tenant_id,
            rule_id,
            result["attempted"],
            result["confirmed"],
            result["failed"],
            result["skipped"],
        )
        return result
