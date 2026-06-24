#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:  # pragma: no cover - depends on prod env
    psycopg = None
    dict_row = None
    DRIVER_ERROR = exc
else:
    DRIVER_ERROR = None


def _json_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
            return loaded if isinstance(loaded, list) else []
        except Exception:
            return []
    return []


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
        f"host={os.environ['APP_PG_HOST']} "
        f"port={os.environ.get('APP_PG_PORT', '5432')} "
        f"dbname={os.environ['APP_PG_DB']} "
        f"user={os.environ['APP_PG_USER']}"
    )
    if os.getenv("APP_PG_PASSWORD"):
        dsn += " password=" + os.environ["APP_PG_PASSWORD"]
    return psycopg.connect(dsn, row_factory=dict_row, connect_timeout=10)


CANDIDATE_SQL = """
WITH album_jobs AS (
    SELECT
        j.id AS job_id,
        j.payload_json,
        j.updated_at AS job_updated_at,
        COALESCE(NULLIF(j.payload_json->>'rule_id', '')::BIGINT, d.rule_id) AS rule_id,
        j.payload_json->>'media_group_id' AS media_group_id,
        ARRAY(
            SELECT jsonb_array_elements_text(COALESCE(j.payload_json->'delivery_ids', '[]'::jsonb))::BIGINT
        ) AS payload_delivery_ids,
        d.id AS anchor_delivery_id
    FROM jobs j
    LEFT JOIN deliveries d ON d.id = NULLIF(j.payload_json->>'delivery_id', '')::BIGINT
    WHERE j.job_type = 'repost_album'
      AND j.status = 'done'
      AND COALESCE(j.error_text, '') = ''
), expanded AS (
    SELECT
        aj.job_id,
        aj.rule_id,
        aj.media_group_id,
        aj.job_updated_at,
        unnest(CASE WHEN cardinality(aj.payload_delivery_ids) > 0 THEN aj.payload_delivery_ids ELSE ARRAY[aj.anchor_delivery_id] END) AS delivery_id
    FROM album_jobs aj
)
SELECT
    e.job_id,
    e.rule_id,
    e.media_group_id,
    e.job_updated_at,
    d.id AS delivery_id,
    d.status,
    d.sent_at,
    d.error_text,
    da.status AS attempt_status,
    da.sent_message_ids_json
FROM expanded e
JOIN deliveries d ON d.id = e.delivery_id
LEFT JOIN LATERAL (
    SELECT status, sent_message_ids_json
    FROM delivery_attempts da
    WHERE da.rule_id = e.rule_id
      AND da.operation_kind = 'album'
      AND COALESCE(da.error_text, '') = ''
      AND (da.delivery_id = d.id OR da.source_message_ids_json ? d.id::TEXT)
    ORDER BY da.updated_at DESC
    LIMIT 1
) da ON TRUE
WHERE d.status = 'processing'
  AND d.sent_at IS NULL
  AND COALESCE(d.error_text, '') = ''
ORDER BY e.rule_id, e.media_group_id, e.job_id, d.id
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run/apply ремонт зависших deliveries после done repost_album jobs")
    parser.add_argument("--apply", action="store_true", help="применить безопасный ремонт")
    parser.add_argument("--rule-id", type=int, help="ограничить конкретным rule_id")
    parser.add_argument("--delivery-id", type=int, action="append", help="ограничить delivery_id; можно указать несколько раз")
    args = parser.parse_args()

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(CANDIDATE_SQL)
            rows = list(cur.fetchall())
            if args.rule_id is not None:
                rows = [r for r in rows if int(r["rule_id"] or 0) == args.rule_id]
            if args.delivery_id:
                allowed = {int(x) for x in args.delivery_id}
                rows = [r for r in rows if int(r["delivery_id"]) in allowed]

            print(f"Найдено кандидатов: {len(rows)}")
            for row in rows:
                sent_ids = _json_list(row.get("sent_message_ids_json"))
                action = "sent" if row.get("attempt_status") in {"accepted", "verified"} and sent_ids else "pending"
                print(
                    "candidate "
                    f"job_id={row['job_id']} rule_id={row['rule_id']} delivery_id={row['delivery_id']} "
                    f"media_group_id={row['media_group_id']} attempt_status={row.get('attempt_status')} "
                    f"sent_message_ids={sent_ids} repair_action={action}"
                )

            if not args.apply:
                print("DRY-RUN: изменения не применялись. Добавьте --apply для ремонта.")
                return 0

            now = datetime.now(timezone.utc).isoformat()
            repaired = 0
            for row in rows:
                sent_ids = _json_list(row.get("sent_message_ids_json"))
                if row.get("attempt_status") in {"accepted", "verified"} and sent_ids:
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'sent', sent_at = %s, error_text = NULL,
                            sent_message_id = %s, sent_message_ids_json = %s::jsonb,
                            delivery_method = 'repair_repost_album_done_job'
                        WHERE id = %s AND status = 'processing' AND sent_at IS NULL AND COALESCE(error_text, '') = ''
                        """,
                        (now, int(sent_ids[0]), json.dumps(sent_ids), int(row["delivery_id"])),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'pending', error_text = NULL
                        WHERE id = %s AND status = 'processing' AND sent_at IS NULL AND COALESCE(error_text, '') = ''
                        """,
                        (int(row["delivery_id"]),),
                    )
                repaired += cur.rowcount
            conn.commit()
            print(f"APPLY: исправлено deliveries: {repaired}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
