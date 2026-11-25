#!/usr/bin/env python3
"""Inspect and acknowledge a durable inbox incident after offline operator review.

No Telegram calls or replay. Stop all publication workers before applying.
An acknowledgment does not resolve any pending receipts or prove delivery.
"""
from __future__ import annotations

import argparse
import getpass
import json
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("account_id", type=int)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--workers-stopped", action="store_true")
    parser.add_argument("--accept-unresolved-no-resend", action="store_true")
    parser.add_argument("--expected-blocked-at")
    parser.add_argument("--note", default="")
    args = parser.parse_args()
    if args.account_id <= 0:
        parser.error("Account ID must be positive")
    if args.apply and not (args.workers_stopped and args.accept_unresolved_no_resend
                           and args.expected_blocked_at and args.note.strip()):
        parser.error("Apply requires stopped workers, no replay, exact timestamp and review note")
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")
    sys.path.insert(0, str(root))
    from app.postgres_repository import PostgresRepository

    repo = PostgresRepository()
    try:
        incident = repo.get_publication_inbox_incident(args.account_id)
        if not incident:
            print("Incident not found")
            return 1
        print(json.dumps(incident, default=str, ensure_ascii=False))
        if not args.apply:
            return 0
        expected = datetime.fromisoformat(args.expected_blocked_at)
        if expected.tzinfo is None:
            parser.error("Timestamp must include timezone")
        if not repo.acknowledge_publication_inbox_incident(
            args.account_id, expected_blocked_at=expected,
            note=f"{getpass.getuser()}: {args.note.strip()}",
        ):
            print("Refused: incident changed or was already acknowledged")
            return 1
        print("Acknowledged. Restart workers; unresolved receipts remain quarantined.")
        return 0
    finally:
        repo.client.close_all_connections()


if __name__ == "__main__":
    raise SystemExit(main())
