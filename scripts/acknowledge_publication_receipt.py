#!/usr/bin/env python3
"""Inspect an expired publication receipt; explicitly quarantine after manual review.

No Telegram calls, sends, edits or deletions. Dry-run unless both apply flags
and a review note are supplied. Run only as an authorized database operator.
"""
from __future__ import annotations

import argparse
import getpass
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("key", help="Exact publication receipt idempotency key")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--accept-no-resend", action="store_true")
    parser.add_argument("--note", default="", help="Result of manual review; no credentials")
    args = parser.parse_args()
    if args.apply and (not args.accept_no_resend or not args.note.strip()):
        parser.error("--apply requires --accept-no-resend and --note")
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from app.postgres_repository import PostgresRepository

    repo = PostgresRepository()
    try:
        receipt = repo.get_publication_receipt(args.key)
        if not receipt:
            print("Receipt not found")
            return 1
        fields = ("rule_id", "source_channel", "source_thread_id", "source_message_ids",
                  "target_peer_id", "target_thread_id", "scheduled_ids", "published_ids",
                  "expired_at", "manual_review")
        print(json.dumps({name: receipt[name] for name in fields}, default=str, ensure_ascii=False))
        if not args.apply:
            return 0
        if not repo.acknowledge_expired_publication_receipt(
            args.key, operator=getpass.getuser(), note=args.note,
        ):
            print("Refused: receipt must be expired, unresolved, accepted, with paused idle rule and intact faulty deliveries.")
            return 1
        print("Reviewed and quarantined. No messages sent; accepted evidence retained.")
        return 0
    finally:
        repo.client.close_thread_connection()


if __name__ == "__main__":
    raise SystemExit(main())
