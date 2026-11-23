#!/usr/bin/env python3
"""Read-only production delivery audit: faulty deliveries, attempts, jobs, self-loop and fixed rules.
No Telegram calls, no DB modifications, no captions/media/token export.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

ROOT = Path.cwd()
load_dotenv(ROOT / ".env")
REDACTIONS = (
    (re.compile(r"(?i)(bot[0-9]{6,}:)[A-Za-z0-9_-]{15,}"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._=-]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)((?:api[_ -]?key|password|token|secret)\s*[:=]\s*)[^\s,;]+"), r"\1[REDACTED]"),
)


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items() if k not in {"content_json", "payload_json", "caption", "text"}}
    if isinstance(value, list):
        return [clean(x) for x in value]
    if isinstance(value, str):
        for pattern, replacement in REDACTIONS:
            value = pattern.sub(replacement, value)
        return value[:1200]
    if isinstance(value, (datetime,)):
        return value.isoformat()
    return value


def db_conn():
    required = ("APP_PG_HOST", "APP_PG_DB", "APP_PG_USER")
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError("Не найдены DB env: " + ", ".join(missing) + " (значения не выводятся)")
    return psycopg.connect(
        host=os.environ["APP_PG_HOST"],
        port=os.getenv("APP_PG_PORT", "5432"),
        dbname=os.environ["APP_PG_DB"],
        user=os.environ["APP_PG_USER"],
        password=os.getenv("APP_PG_PASSWORD", ""),
        row_factory=dict_row,
        autocommit=True,
        connect_timeout=7,
        options="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=2000",
        application_name="vimi_delivery_readonly_audit",
    )


QUERIES = {
    "rule_inventory": """
        SELECT r.id rule_id, r.mode, r.schedule_mode, r.is_active,
               r.source_id, r.source_thread_id, r.target_id, r.target_thread_id,
               (r.source_id = r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               (r.source_id = r.target_id AND r.source_thread_id IS DISTINCT FROM r.target_thread_id) same_chat_cross_topic,
               r.repost_caption_enabled,
               r.repost_caption_delivery_mode, r.caption_delivery_mode, r.video_caption_delivery_mode,
               (LENGTH(COALESCE(r.repost_caption,'')) > 0) has_repost_caption,
               r.fixed_times_json, r.next_run_at
        FROM routing r
        ORDER BY r.id
    """,
    "delivery_status_by_rule": """
        SELECT d.rule_id, r.mode, r.schedule_mode,
               (r.source_id = r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               COUNT(*) total, COUNT(*) FILTER(WHERE d.status='faulty') faulty,
               COUNT(*) FILTER(WHERE d.status='pending') pending,
               COUNT(*) FILTER(WHERE d.status='processing') processing,
               COUNT(*) FILTER(WHERE d.status='sent') sent,
               COUNT(*) FILTER(WHERE d.created_at >= %s) created_recent,
               MAX(d.created_at) latest_created
        FROM deliveries d JOIN routing r ON r.id=d.rule_id
        GROUP BY d.rule_id, r.mode, r.schedule_mode, r.source_id, r.target_id, r.source_thread_id, r.target_thread_id
        ORDER BY faulty DESC, processing DESC, d.rule_id
    """,
    "faulty_deliveries_recent": """
        SELECT d.id delivery_id, d.rule_id, d.post_id, d.status, d.created_at,
               d.sent_at, d.attempt_count, d.error_text, d.delivery_method,
               d.sent_message_id, d.sent_message_ids_json, d.target_id_snapshot,
               p.message_id source_message_id, p.source_channel, p.source_thread_id,
               p.media_group_id,
               r.mode, r.schedule_mode, r.source_id, r.target_id, r.target_thread_id,
               (r.source_id = r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               COUNT(*) OVER () matching_total
        FROM deliveries d
        JOIN posts p ON p.id=d.post_id JOIN routing r ON r.id=d.rule_id
        WHERE d.status='faulty' AND d.created_at >= %s
        ORDER BY d.id DESC LIMIT %s
    """,
    "recent_error_audit": """
        SELECT a.id audit_id, a.created_at, a.event_type, a.rule_id, a.delivery_id, a.post_id,
               a.status, a.error_text, a.source_id, a.source_thread_id,
               a.target_id, a.target_thread_id,
               r.schedule_mode, r.mode,
               (r.source_id=r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               COUNT(*) OVER () matching_total
        FROM audit_log a LEFT JOIN routing r ON r.id=a.rule_id
        WHERE a.created_at >= %s
          AND (NULLIF(BTRIM(COALESCE(a.error_text,'')), '') IS NOT NULL
               OR LOWER(COALESCE(a.status,'')) IN ('faulty','failed','error')
               OR a.event_type ILIKE '%%exception%%' OR a.event_type ILIKE '%%failed%%')
        ORDER BY a.id DESC LIMIT %s
    """,
    "recent_job_errors": """
        SELECT j.id job_id, j.job_type, j.queue, j.status, j.attempts, j.max_attempts,
               j.created_at, j.updated_at, j.error_text,
               j.payload_json->>'delivery_id' delivery_id,
               j.payload_json->>'rule_id' rule_id,
               j.dedup_key,
               COUNT(*) OVER () matching_total
        FROM jobs j
        WHERE j.updated_at >= %s
          AND (j.status IN ('failed','retry')
               OR NULLIF(BTRIM(COALESCE(j.error_text,'')), '') IS NOT NULL)
        ORDER BY j.updated_at DESC LIMIT %s
    """,
    "recent_attempt_failures": """
        SELECT da.id attempt_id, da.delivery_id, da.rule_id, da.job_id,
               da.operation_kind, da.status, da.telegram_method,
               da.created_at, da.updated_at, da.error_text,
               da.sent_message_ids_json, da.source_message_ids_json,
               r.schedule_mode, r.mode,
               (r.source_id=r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               COUNT(*) OVER () matching_total
        FROM delivery_attempts da LEFT JOIN routing r ON r.id=da.rule_id
        WHERE da.updated_at >= %s
          AND (da.status IN ('failed_before_send','failed_after_send')
               OR NULLIF(BTRIM(COALESCE(da.error_text,'')), '') IS NOT NULL)
        ORDER BY da.updated_at DESC LIMIT %s
    """,
    "uncertain_attempts": """
        SELECT da.id attempt_id, da.rule_id, da.delivery_id, da.job_id,
               da.status, da.operation_kind, da.telegram_method,
               da.sent_message_ids_json, da.updated_at,
               d.status delivery_status, d.sent_message_id,
               r.schedule_mode, r.mode,
               COUNT(*) OVER () matching_total
        FROM delivery_attempts da
        LEFT JOIN deliveries d ON d.id=da.delivery_id
        LEFT JOIN routing r ON r.id=da.rule_id
        WHERE da.updated_at >= %s
          AND da.updated_at < NOW() - INTERVAL '10 minutes'
          AND da.status IN ('sending','accepted','failed_after_send')
        ORDER BY da.updated_at DESC LIMIT %s
    """,
    "caption_and_self_loop_audit": """
        SELECT a.id audit_id, a.created_at, a.event_type, a.rule_id, a.delivery_id,
               a.status, a.error_text,
               r.schedule_mode, r.mode,
               (r.source_id=r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               COUNT(*) OVER () matching_total
        FROM audit_log a LEFT JOIN routing r ON r.id=a.rule_id
        WHERE a.created_at >= %s
          AND (a.event_type ILIKE '%%caption%%'
            OR a.event_type ILIKE '%%edit%%'
            OR a.event_type ILIKE '%%self_loop%%'
            OR a.error_text ILIKE '%%caption%%'
            OR a.error_text ILIKE '%%edit_message%%'
            OR a.error_text ILIKE '%%self_loop%%')
        ORDER BY a.id DESC LIMIT %s
    """,
    "recent_problem_state": """
        SELECT problem_key, problem_type, rule_id, delivery_id, first_seen_at,
               last_seen_at, hit_count, is_active, is_muted
        FROM problem_state
        WHERE last_seen_at >= %s
        ORDER BY last_seen_at DESC
    """,
    "duplicate_target_message_ids": """
        SELECT d.target_id_snapshot target_id, d.sent_message_id,
               COUNT(*) delivery_rows, COUNT(DISTINCT d.rule_id) rules_count,
               ARRAY_AGG(d.id ORDER BY d.id) delivery_ids,
               ARRAY_AGG(d.rule_id ORDER BY d.id) rule_ids
        FROM deliveries d
        WHERE d.created_at >= %s
          AND d.sent_message_id IS NOT NULL
          AND d.target_id_snapshot IS NOT NULL
        GROUP BY d.target_id_snapshot, d.sent_message_id
        HAVING COUNT(*) > 1
        ORDER BY delivery_rows DESC LIMIT 300
    """,
    "reingested_source_message_ids": """
        SELECT p.source_channel, p.source_thread_id, p.message_id,
               COUNT(*) saved_post_rows, ARRAY_AGG(p.id ORDER BY p.id) post_ids
        FROM posts p
        WHERE p.created_at >= %s
        GROUP BY p.source_channel, p.source_thread_id, p.message_id
        HAVING COUNT(*) > 1
        ORDER BY saved_post_rows DESC LIMIT 300
    """,
    "fixed_sent_receipts_recent": """
        SELECT d.id delivery_id, d.rule_id, d.post_id, d.status,
               d.created_at, d.sent_at, d.attempt_count, d.delivery_method,
               d.sent_message_id, d.sent_message_ids_json, d.target_id_snapshot,
               p.message_id source_message_id, p.source_channel, p.source_thread_id,
               p.media_group_id, r.mode, r.schedule_mode,
               r.source_id, r.target_id, r.target_thread_id,
               (r.source_id=r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               COUNT(*) OVER () matching_total
        FROM deliveries d JOIN posts p ON p.id=d.post_id
        JOIN routing r ON r.id=d.rule_id
        WHERE r.schedule_mode='fixed' AND d.status='sent' AND d.created_at >= %s
        ORDER BY d.id DESC LIMIT %s
    """,
    "fixed_repeated_source_candidates": """
        SELECT d.rule_id, p.source_channel, p.source_thread_id, p.message_id source_message_id,
               COUNT(*) delivery_rows, ARRAY_AGG(d.id ORDER BY d.id) delivery_ids,
               ARRAY_AGG(d.sent_message_id ORDER BY d.id) target_message_ids
        FROM deliveries d JOIN routing r ON r.id=d.rule_id
        JOIN posts p ON p.id=d.post_id
        WHERE r.schedule_mode='fixed' AND d.status='sent' AND d.created_at >= %s
        GROUP BY d.rule_id, p.source_channel, p.source_thread_id, p.message_id
        HAVING COUNT(*)>1 ORDER BY delivery_rows DESC LIMIT 300
    """,
    "fixed_rule_delivery_summary": """
        SELECT r.id rule_id, r.mode, r.schedule_mode, r.source_id, r.target_id,
               r.source_thread_id, r.target_thread_id,
               (r.source_id=r.target_id AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id) self_loop_exact,
               r.repost_caption_enabled, r.repost_caption_delivery_mode, r.caption_delivery_mode,
               COUNT(d.id) recent_deliveries,
               COUNT(d.id) FILTER(WHERE d.status='faulty') recent_faulty,
               COUNT(d.id) FILTER(WHERE d.status='sent') recent_sent,
               COUNT(d.id) FILTER(WHERE d.status='processing') recent_processing,
               COUNT(d.id) FILTER(WHERE d.sent_message_id IS NOT NULL) with_saved_target_message_id
        FROM routing r LEFT JOIN deliveries d ON d.rule_id=r.id AND d.created_at >= %s
        WHERE r.schedule_mode='fixed'
        GROUP BY r.id
        ORDER BY recent_faulty DESC, recent_sent DESC
    """,
}


def execute(cur, sql, params=()):
    cur.execute(sql, params)
    return [clean(dict(row)) for row in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=10000, help="maximum per detailed section")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    if not 1 <= args.days <= 365 or not 10 <= args.limit <= 100000:
        ap.error("days=1..365; limit=10..100000")

    started = datetime.now(timezone.utc)
    start_dt = started - timedelta(days=args.days)
    start_iso = start_dt.isoformat()
    report = {
        "generated_at_utc": started.isoformat(),
        "period_days": args.days,
        "since_utc": start_iso,
        "read_only": True,
        "notes": [
            "Failure samples may be capped; matching_total tells how many rows matched.",
            "Historical failed attempts remain even if a delivery later succeeded.",
            "Same chat with different topics is NOT the same as exact self-loop.",
            "Database-only findings are candidates, not proof of duplicate Telegram posts.",
            "No post content, captions, media, or Telegram tokens are exported.",
        ],
        "sections": {},
        "errors": {},
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            read_only = cur.fetchone()["transaction_read_only"]
            if str(read_only).lower() != "on":
                raise RuntimeError("Connection is not read-only; audit aborted")
            print("DB connection read-only confirmed.", flush=True)
            for name, sql in QUERIES.items():
                try:
                    if name in ("rule_inventory",):
                        params = ()
                    elif name in ("recent_problem_state", "duplicate_target_message_ids",
                                  "reingested_source_message_ids", "fixed_rule_delivery_summary",
                                  "fixed_repeated_source_candidates", "delivery_status_by_rule"):
                        params = (start_iso,)
                    elif name in ("recent_job_errors", "recent_attempt_failures", "uncertain_attempts"):
                        params = (start_dt, args.limit)
                    else:
                        params = (start_iso, args.limit)
                    rows = execute(cur, sql, params)
                    report["sections"][name] = rows
                    n = rows[0].get("matching_total", len(rows)) if rows else 0
                    print(f"  {name}: {len(rows)} rows (total={n})", flush=True)
                except Exception as exc:
                    # No credentials or SQL text exposed. Each SELECT runs in its own
                    # read-only autocommit transaction; one failure doesn't stop the audit.
                    report["errors"][name] = clean(f"{exc.__class__.__name__}: {str(exc).splitlines()[0]}")
                    print(f"  {name}: ERROR ({exc.__class__.__name__})", flush=True)

    categories = Counter()
    for sec in ("faulty_deliveries_recent", "recent_error_audit",
                "recent_job_errors", "recent_attempt_failures"):
        for r in report["sections"].get(sec, []):
            msg = str(r.get("error_text") or "").lower()
            if any(x in msg for x in ("self_loop", "self-loop", "same chat")):
                cat = "self_loop"
            elif any(x in msg for x in ("caption", "edit_message", "entitybounds", "message not modified")):
                cat = "caption_or_edit"
            elif any(x in msg for x in ("flood", "retry after", "retryafter", "rate limit", "too many requests")):
                cat = "telegram_rate_limit"
            elif any(x in msg for x in ("file", "media", "download", "upload", "too large", "empty")):
                cat = "file_or_media"
            elif any(x in msg for x in ("timeout", "timed out", "connection", "network", "unavailable")):
                cat = "network_or_timeout"
            elif any(x in msg for x in ("not found", "forbidden", "private", "access", "permission")):
                cat = "permissions_or_deleted"
            else:
                cat = "other"
            categories[cat] += 1
    report["rough_error_categories_from_exported_rows"] = dict(categories)
    out = Path(args.out).expanduser() if args.out else Path("/tmp") / f"vimi_delivery_audit_{started:%Y%m%d_%H%M%S}.json"
    oldmask = os.umask(0o077)
    try:
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        os.chmod(out, 0o600)
    finally:
        os.umask(oldmask)
    print("\n=== SUMMARY ===")
    for k, v in categories.most_common():
        print(f"{k}: {v}")
    rules = report["sections"].get("rule_inventory", [])
    print(f"rules={len(rules)}; exact_self_loop={sum(bool(r['self_loop_exact']) for r in rules)}; "
          f"same_chat_cross_topic={sum(bool(r['same_chat_cross_topic']) for r in rules)}; "
          f"fixed={sum(r['schedule_mode']=='fixed' for r in rules)}")
    print("query_errors:", len(report["errors"]))
    print("REPORT:", out)
    print("No rows modified. Upload this JSON report to ChatGPT for incident classification.")


if __name__ == "__main__":
    main()
