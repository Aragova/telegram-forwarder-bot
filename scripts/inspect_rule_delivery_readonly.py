#!/usr/bin/env python3
"""Read-only investigation of one delivery (no Telegram writes, secrets or message bodies)."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


def clean(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean(v) for v in value]
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rule", type=int, required=True)
    parser.add_argument("--message", type=int, required=True)
    args = parser.parse_args()
    load_dotenv(Path.cwd() / ".env")

    with psycopg.connect(
        host=os.environ["APP_PG_HOST"],
        port=os.getenv("APP_PG_PORT", "5432"),
        dbname=os.environ["APP_PG_DB"],
        user=os.environ["APP_PG_USER"],
        password=os.getenv("APP_PG_PASSWORD", ""),
        row_factory=dict_row,
        autocommit=True,
        connect_timeout=7,
        options="-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=1000",
        application_name="vimi_single_delivery_readonly",
    ) as connection:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT d.id AS delivery_id, d.rule_id, p.message_id AS source_message_id,
                       d.status AS delivery_status, d.error_text AS delivery_error,
                       d.attempt_count, d.created_at, d.sent_at,
                       d.sent_message_id, d.sent_message_ids_json, d.delivery_method,
                       r.mode, r.schedule_mode, r.caption_delivery_mode,
                       r.repost_caption_delivery_mode, r.repost_caption_enabled,
                       (r.source_id = r.target_id
                          AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) AS self_loop
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                JOIN routing r ON r.id = d.rule_id
                WHERE d.rule_id = %s AND p.message_id = %s
                ORDER BY d.id DESC LIMIT 5
            """, (args.rule, args.message))
            deliveries = cur.fetchall()
            result = {"rule": args.rule, "source_message": args.message,
                      "deliveries": [], "warning": None}
            if not deliveries:
                result["warning"] = "No matching delivery; check rule/message ID"
            for delivery in deliveries:
                did = int(delivery["delivery_id"])
                cur.execute("""
                    SELECT id AS attempt_id, status AS attempt_status, operation_kind,
                           telegram_method, created_at, updated_at, error_text AS attempt_error,
                           source_message_ids_json, sent_message_ids_json
                    FROM delivery_attempts
                    WHERE delivery_id = %s
                    ORDER BY id DESC LIMIT 10
                """, (did,))
                attempts = cur.fetchall()

                cur.execute("""
                    SELECT id AS audit_id, created_at, event_type, status, error_text,
                           extra_json::jsonb->>'pipeline_stage' AS pipeline_stage,
                           extra_json::jsonb->>'pipeline_result' AS pipeline_result,
                           extra_json::jsonb->>'resolution_method' AS resolution_method,
                           extra_json::jsonb->>'returned_candidate_id' AS returned_candidate_id,
                           extra_json::jsonb->>'sent_message_id' AS audit_sent_id,
                           extra_json::jsonb->>'skip_reason' AS skip_reason
                    FROM audit_log
                    WHERE delivery_id = %s
                    ORDER BY id DESC LIMIT 45
                """, (did,))
                audits = cur.fetchall()

                cur.execute("""
                    SELECT id AS job_id, job_type, status AS job_status, attempts, max_attempts,
                           created_at, updated_at, error_text AS job_error
                    FROM jobs
                    WHERE payload_json->>'delivery_id' = %s
                       OR (
                          jsonb_typeof(payload_json->'delivery_ids') = 'array'
                          AND EXISTS (
                            SELECT 1 FROM jsonb_array_elements_text(
                              COALESCE(payload_json->'delivery_ids', '[]'::jsonb)
                            ) item(value)
                            WHERE item.value = %s
                          )
                       )
                    ORDER BY id DESC LIMIT 20
                """, (str(did),str(did)))
                jobs = cur.fetchall()
                result["deliveries"].append({
                    "delivery": clean(delivery),
                    "attempts": clean(attempts),
                    "audit_events_latest_first": clean(audits),
                    "jobs": clean(jobs),
                })
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
