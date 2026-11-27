#!/usr/bin/env python3
"""Audit/reconcile album receipts orphaned by historical full rescans.

Default: read-only. Apply requires two explicit flags. Never sends messages.
No database mutations occur until each album has passed live MTProto checks.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
import tempfile
from collections import Counter, OrderedDict
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from app.config import settings  # noqa: E402
from app.postgres_repository import PostgresRepository  # noqa: E402


def _ids(value) -> list[int]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return []
    if not isinstance(value, list):
        return []
    try:
        result = [int(x) for x in value]
    except (ValueError, TypeError):
        return []
    return result if all(x > 0 for x in result) else []


def build_candidates(rows: list[dict]) -> list[dict]:
    """Group by persisted attempt (not individual delivery rows)."""
    grouped: OrderedDict[tuple, list[dict]] = OrderedDict()
    for row in rows:
        group = (
            int(row["attempt_id"]),
            str(row["source_channel"]),
            row["source_thread_id"],
            str(row["media_group_id"]),
            str(row["target_id"]),
            row["target_thread_id"],
        )
        grouped.setdefault(group, []).append(row)

    candidates = []
    for (attempt_id, source, thread, mgid, target, target_thread), members in grouped.items():
        members.sort(key=lambda item: int(item["message_id"]))
        sent_ids = _ids(members[0]["sent_message_ids_json"])
        source_ids = [int(x["message_id"]) for x in members]
        if (not sent_ids or len(sent_ids) != len(source_ids)
                or len(set(sent_ids)) != len(sent_ids)
                or sent_ids != sorted(sent_ids)
                or any(row["attempt_status"] != members[0]["attempt_status"] for row in members)):
            print("SKIP_INCOMPLETE_LEDGER:", attempt_id, mgid, flush=True)
            continue
        candidates.append({
            "attempt_id": attempt_id,
            "source_channel": source,
            "source_thread_id": thread,
            "target_id": target,
            "target_thread_id": target_thread,
            "media_group_id": mgid,
            "source_ids": source_ids,
            "delivery_ids": [int(x["delivery_id"]) for x in members],
            "target_ids": sent_ids,
        })
    return candidates


def _media_kind(message) -> str:
    if message.photo:
        return "photo"
    if message.document:
        return str(message.document.mime_type or "document")
    return "other"


def _media_identity(message) -> tuple[str, int] | None:
    for kind in ("photo", "document"):
        media = getattr(message, kind, None)
        media_id = getattr(media, "id", None)
        if type(media_id) is int and media_id != 0:
            return kind, media_id
    return None


def validate_candidate(candidate: dict, sources: dict, targets: dict, *,
                       exact_media: bool = False) -> str:
    source_ids = candidate["source_ids"]
    target_ids = candidate["target_ids"]
    if len(source_ids) != len(target_ids):
        return "COUNT_MISMATCH"
    if any(mid not in sources for mid in source_ids):
        return "SOURCE_MISSING"
    if any(mid not in targets for mid in target_ids):
        return "TARGET_MISSING"
    src = [sources[x] for x in source_ids]
    dst = [targets[x] for x in target_ids]
    groups = {getattr(m, "grouped_id", None) for m in dst}
    if None in groups or len(groups) != 1:
        return "TARGET_GROUP_MISMATCH"
    if len(src) > 1:
        source_groups = {getattr(m, "grouped_id", None) for m in src}
        if None in source_groups or len(source_groups) != 1:
            return "SOURCE_GROUP_MISMATCH"
    if [_media_kind(m) for m in src] != [_media_kind(m) for m in dst]:
        return "MEDIA_MISMATCH"
    source_caption = " ".join((m.message or "").strip() for m in src if m.message).strip()
    target_caption = " ".join((m.message or "").strip() for m in dst if m.message).strip()
    if source_caption != target_caption:
        return "CAPTION_MISMATCH"
    if candidate["target_thread_id"] is not None:
        # Telegram forum replies require a separate topic-specific verifier.
        return "TOPIC_REQUIRES_MANUAL_REVIEW"
    if exact_media:
        if not 2 <= len(src) <= 10 or any(
            str(getattr(message, "grouped_id", None)) != candidate["media_group_id"]
            for message in src
        ):
            return "SOURCE_GROUP_ID_MISMATCH"
        for source, target in zip(src, dst):
            identity = _media_identity(source)
            if identity is None or identity != _media_identity(target):
                return "MEDIA_IDENTITY_MISMATCH"
            if (source.message or "") != (target.message or ""):
                return "MEMBER_CAPTION_MISMATCH"
        return "EXACT_MEDIA_OK"
    return "STRUCTURE_OK"


async def _fetch(client, entity, ids: list[int], label: str, max_wait: int) -> dict:
    found = {}
    ids = sorted(set(ids))
    for offset in range(0, len(ids), 90):
        batch = ids[offset:offset + 90]
        for attempt in range(2):
            try:
                messages = await asyncio.wait_for(
                    client.get_messages(entity, ids=batch), timeout=120
                )
                for message in messages or []:
                    if message:
                        found[int(message.id)] = message
                break
            except FloodWaitError as exc:
                seconds = int(exc.seconds)
                print(f"{label} FLOOD_WAIT={seconds}", flush=True)
                if seconds > max_wait or attempt:
                    raise RuntimeError("Telegram flood limit; no unverified rows updated") from exc
                await asyncio.sleep(seconds + 5)
        print(
            f"{label} CHECKED={min(offset + len(batch), len(ids))}/{len(ids)}",
            flush=True,
        )
        if offset + 90 < len(ids):
            await asyncio.sleep(5)
    return found


async def audit(args) -> None:
    repo = PostgresRepository()
    faulty_group = args.recover_faulty
    candidates = build_candidates(repo.get_pending_album_ledger_candidates(
        args.rule_id, faulty_media_group_id=faulty_group,
    ))
    if faulty_group and len(candidates) > 1:
        raise RuntimeError("Multiple receipts for the selected album; no recovery")
    print(f"RULE={args.rule_id} CANDIDATES={len(candidates)}", flush=True)
    if not candidates:
        return
    if args.apply and not faulty_group and not args.accept_structural_match:
        raise RuntimeError(
            "--apply requires --accept-structural-match: media bytes are NOT compared"
        )

    session = Path(settings.session_name).expanduser()
    if session.suffix != ".session":
        session = Path(str(session) + ".session")
    if not session.is_absolute():
        session = ROOT / session
    if not session.is_file():
        raise RuntimeError("Telethon session not found; do not create a new login")

    # A copied SQLite session cannot lock the running sender's SQLite connection.
    with tempfile.TemporaryDirectory(prefix="vimi-album-ledger-") as tmp:
        clone = Path(tmp) / "audit.session"
        with sqlite3.connect(f"file:{session}?mode=ro", uri=True) as src:
            with sqlite3.connect(clone) as dst:
                src.backup(dst)
        client = TelegramClient(
            str(clone.with_suffix("")),
            settings.api_id, settings.api_hash,
            flood_sleep_threshold=0,
        )
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise RuntimeError("Copied Telethon session is unauthorized")

            source_channels = {x["source_channel"] for x in candidates}
            targets = {x["target_id"] for x in candidates}
            if len(source_channels) != 1 or len(targets) != 1:
                raise RuntimeError("Mixed-source/target candidates require separate audits")
            source_peer = await client.get_input_entity(int(next(iter(source_channels))))
            target_peer = await client.get_input_entity(int(next(iter(targets))))
            source_ids = [mid for x in candidates for mid in x["source_ids"]]
            target_ids = [mid for x in candidates for mid in x["target_ids"]]
            sources = await _fetch(client, source_peer, source_ids, "SOURCE", args.max_flood_wait)
            await asyncio.sleep(5)
            dest = await _fetch(client, target_peer, target_ids, "TARGET", args.max_flood_wait)

            counts = Counter()
            for item in candidates:
                verdict = validate_candidate(item, sources, dest, exact_media=bool(faulty_group))
                counts[verdict] += 1
                outcome = "DRY_RUN"
                expected_verdict = "EXACT_MEDIA_OK" if faulty_group else "STRUCTURE_OK"
                if verdict == expected_verdict and args.apply:
                    try:
                        updated = await asyncio.to_thread(
                            repo.reconcile_confirmed_album_ledger,
                            rule_id=args.rule_id,
                            attempt_id=item["attempt_id"],
                            source_channel=item["source_channel"],
                            source_thread_id=item["source_thread_id"],
                            media_group_id=item["media_group_id"],
                            source_message_ids=item["source_ids"],
                            target_message_ids=item["target_ids"],
                            recover_faulty=bool(faulty_group),
                            expected_target_id=item["target_id"],
                            expected_delivery_ids=item["delivery_ids"],
                        )
                        outcome = f"RESTORED_{updated}"
                    except Exception as exc:
                        outcome = f"SKIPPED_DB_GUARD:{type(exc).__name__}:{str(exc)[:130]}"
                        counts["DB_GUARD_REJECTED"] += 1
                print(
                    f"album={item['media_group_id']} attempt={item['attempt_id']} "
                    f"verify={verdict} outcome={outcome}",
                    flush=True,
                )
            print("RESULT:", dict(counts), flush=True)
            print("MODE:", "APPLY" if args.apply else "DRY_RUN", flush=True)
            print("AUDIT_COMPLETE", flush=True)
        finally:
            await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rule-id", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--recover-faulty", metavar="MEDIA_GROUP_ID",
        help="Audit a single orphaned legacy faulty album; apply requires a paused rule "
             "and exact Telegram media identities, not just structural similarity",
    )
    parser.add_argument(
        "--accept-structural-match", action="store_true",
        help="Explicitly accept verified IDs, media types, album groups and captions (not byte-identical media)",
    )
    parser.add_argument("--max-flood-wait", type=int, default=120)
    args = parser.parse_args()
    if args.recover_faulty and args.accept_structural_match:
        parser.error("--recover-faulty never accepts structural-only matches")
    asyncio.run(audit(args))


if __name__ == "__main__":
    main()
