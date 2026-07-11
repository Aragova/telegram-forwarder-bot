#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:  # pragma: no cover
    psycopg = None
    dict_row = None
    DRIVER_ERROR = exc
else:
    DRIVER_ERROR = None

NOOP_METHODS = ("self_loop_noop_single", "self_loop_noop_album")

FIND_SQL = """
SELECT
    d.id AS delivery_id,
    d.rule_id,
    p.message_id AS source_message_id,
    d.delivery_method AS noop_method,
    d.sent_at,
    d.status,
    da.id AS attempt_id,
    j.id AS job_id
FROM deliveries d
JOIN posts p ON p.id = d.post_id
LEFT JOIN LATERAL (
    SELECT id
    FROM delivery_attempts da
    WHERE da.delivery_id = d.id
      AND da.telegram_method = ANY(%s)
    ORDER BY da.updated_at DESC, da.id DESC
    LIMIT 1
) da ON TRUE
LEFT JOIN LATERAL (
    SELECT id
    FROM jobs j
    WHERE j.status = 'done'
      AND NULLIF(j.payload_json->>'delivery_id', '')::BIGINT = d.id
    ORDER BY j.updated_at DESC, j.id DESC
    LIMIT 1
) j ON TRUE
WHERE d.status = 'sent'
  AND d.delivery_method = ANY(%s)
  AND (%s::BIGINT IS NULL OR d.rule_id = %s::BIGINT)
ORDER BY d.rule_id, d.id
"""


def _connect():
    load_dotenv(ROOT / ".env")
    if os.getenv("APP_DB_BACKEND", "").strip().lower() != "postgres":
        raise RuntimeError("APP_DB_BACKEND должен быть postgres")
    if psycopg is None:
        raise RuntimeError(f"Не установлен psycopg: {DRIVER_ERROR}")
    required = ["APP_PG_HOST", "APP_PG_PORT", "APP_PG_DB", "APP_PG_USER"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError("Не заданы переменные PostgreSQL: " + ", ".join(missing))
    dsn = (
        f"host={os.environ['APP_PG_HOST']} port={os.environ.get('APP_PG_PORT', '5432')} "
        f"dbname={os.environ['APP_PG_DB']} user={os.environ['APP_PG_USER']}"
    )
    if os.getenv("APP_PG_PASSWORD"):
        dsn += " password=" + os.environ["APP_PG_PASSWORD"]
    return psycopg.connect(dsn, row_factory=dict_row, connect_timeout=10)


def filter_candidates(rows, rule_id=None):
    result = []
    for row in rows:
        if row.get("status") != "sent":
            continue
        if row.get("noop_method") not in NOOP_METHODS:
            continue
        if rule_id is not None and int(row.get("rule_id") or 0) != int(rule_id):
            continue
        result.append(row)
    return result


def print_candidates(rows):
    print(f"Найдено self-loop noop deliveries: {len(rows)}")
    for row in rows:
        print(
            "candidate "
            f"rule_id={row.get('rule_id')} delivery_id={row.get('delivery_id')} "
            f"source_message_id={row.get('source_message_id')} noop_method={row.get('noop_method')} "
            f"sent_at={row.get('sent_at')} status={row.get('status')}"
        )


def apply_repair(cur, rows):
    repaired = 0
    for row in rows:
        delivery_id = int(row["delivery_id"])
        cur.execute(
            """
            UPDATE deliveries
            SET status = 'pending', sent_at = NULL, error_text = NULL,
                sent_message_id = NULL, sent_message_ids_json = NULL,
                target_id_snapshot = NULL, delivery_method = NULL
            WHERE id = %s AND status = 'sent' AND delivery_method = ANY(%s)
            """,
            (delivery_id, list(NOOP_METHODS)),
        )
        if cur.rowcount:
            cur.execute(
                "DELETE FROM delivery_attempts WHERE delivery_id = %s AND telegram_method = ANY(%s)",
                (delivery_id, list(NOOP_METHODS)),
            )
            cur.execute(
                """
                UPDATE jobs
                SET status = 'failed', error_text = 'self-loop noop repair reset delivery', updated_at = NOW()
                WHERE status = 'done' AND NULLIF(payload_json->>'delivery_id', '')::BIGINT = %s
                """,
                (delivery_id,),
            )
            print(
                "SELF_LOOP_NOOP_REPAIR | "
                f"delivery_id={delivery_id} | rule_id={row.get('rule_id')} | "
                f"source_message_id={row.get('source_message_id')} | old_status=sent | new_status=pending"
            )
            repaired += 1
    return repaired


def main() -> int:
    parser = argparse.ArgumentParser(description="Ремонт deliveries, ошибочно списанных через self-loop noop")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="только показать кандидатов")
    mode.add_argument("--apply", action="store_true", help="применить ремонт")
    parser.add_argument("--rule-id", type=int, help="ограничить rule_id")
    args = parser.parse_args()

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(FIND_SQL, (list(NOOP_METHODS), list(NOOP_METHODS), args.rule_id, args.rule_id))
            rows = filter_candidates(list(cur.fetchall()), rule_id=args.rule_id)
            print_candidates(rows)
            if args.dry_run:
                print("DRY-RUN: изменения не применялись. Добавьте --apply для ремонта.")
                return 0
            repaired = apply_repair(cur, rows)
            conn.commit()
            print(f"APPLY: исправлено deliveries: {repaired}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
