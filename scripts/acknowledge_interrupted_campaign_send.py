#!/usr/bin/env python3
"""Inspect a stuck campaign send; release only after verified manual review.

Stop all campaign workers and pause the rule before applying. Never sends,
deletes, edits or retries Telegram messages. Dry-run by default.
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
    parser.add_argument("message_id", type=int)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--workers-stopped", action="store_true")
    parser.add_argument("--no-live-or-scheduled-publication", action="store_true")
    parser.add_argument("--expected-updated-at", help="Exact timestamp shown during dry-run")
    parser.add_argument("--note", default="")
    args = parser.parse_args()
    if args.apply and not (
        args.workers_stopped and args.no_live_or_scheduled_publication and args.note.strip()
        and args.expected_updated_at
    ):
        parser.error("Apply requires stopped workers, verified absence, exact dry-run timestamp and review note")
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")
    sys.path.insert(0, str(root))
    from app.postgres_repository import PostgresRepository

    repo = PostgresRepository()
    try:
        row = repo.get_campaign_run_message(args.message_id)
        if not row:
            print("Campaign placement not found")
            return 1
        fields = ("id", "run_id", "rule_id", "saved_post_id", "target_id", "target_thread_id",
                  "send_status", "sent_message_id", "sent_message_ids_json", "updated_at")
        print(json.dumps({field: row[field] for field in fields}, default=str, ensure_ascii=False))
        if not args.apply:
            return 0
        expected = datetime.fromisoformat(args.expected_updated_at)
        if expected.tzinfo is None:
            parser.error("--expected-updated-at must include timezone")
        if not repo.acknowledge_interrupted_campaign_send(
            args.message_id, expected_updated_at=expected, operator=getpass.getuser(), note=args.note,
        ):
            print("Refused: unchanged abandoned sending row, paused rule and no active campaign launch are required.")
            return 1
        print("Reviewed. Old placement remains failed; no Telegram operation or replay performed.")
        return 0
    finally:
        repo.client.close_thread_connection()


if __name__ == "__main__":
    raise SystemExit(main())
