from __future__ import annotations

from typing import Any

from app.repository_models import utc_now_iso
from app.repository_split_base import RepositorySplitBase


class BillingRepository(RepositorySplitBase):
    def create_billing_event(
        self,
        tenant_id: int,
        event_type: str,
        *,
        event_source: str | None = None,
        amount: float | None = None,
        currency: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int | None:
        with self.connect() as conn:
            row_id = self.fetch_inserted_id(
                conn,
                """
                INSERT INTO billing_events(tenant_id, event_type, event_source, amount, currency, metadata_json, created_at)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(tenant_id),
                    str(event_type),
                    event_source,
                    amount,
                    currency,
                    self.json_dumps(metadata),
                    utc_now_iso(),
                ),
            )
            conn.commit()
            return row_id

    def get_billing_events(self, tenant_id: int, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM billing_events
                    WHERE tenant_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (int(tenant_id), int(limit)),
                )
                rows = cur.fetchall() or []
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata_json"] = self.safe_json_loads(item.get("metadata_json"), {})
            result.append(item)
        return result

    def create_invoice(
        self,
        *,
        tenant_id: int,
        subscription_id: int,
        period_start: str,
        period_end: str,
        status: str,
        currency: str,
        due_at: str | None,
    ) -> int | None:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            row_id = self.fetch_inserted_id(
                conn,
                """
                INSERT INTO invoices(
                    tenant_id, subscription_id, period_start, period_end, status,
                    subtotal, total, currency, created_at, updated_at, due_at
                )
                VALUES(%s, %s, %s, %s, %s, 0, 0, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(tenant_id),
                    int(subscription_id),
                    str(period_start),
                    str(period_end),
                    str(status),
                    str(currency).upper(),
                    now_iso,
                    now_iso,
                    due_at,
                ),
            )
            conn.commit()
            return row_id

    def add_invoice_item(
        self,
        invoice_id: int,
        *,
        item_type: str,
        description: str,
        quantity: int,
        unit_price: float,
        amount: float,
        metadata: dict[str, Any] | None = None,
    ) -> int | None:
        with self.connect() as conn:
            row_id = self.fetch_inserted_id(
                conn,
                """
                INSERT INTO invoice_items(invoice_id, item_type, description, quantity, unit_price, amount, metadata_json)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(invoice_id),
                    str(item_type),
                    str(description),
                    int(quantity),
                    unit_price,
                    amount,
                    self.json_dumps(metadata or {}),
                ),
            )
            conn.commit()
            return row_id

    def recalculate_invoice_totals(self, invoice_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH sums AS (
                        SELECT COALESCE(SUM(amount), 0) AS amount_sum
                        FROM invoice_items
                        WHERE invoice_id = %s
                    )
                    UPDATE invoices
                    SET subtotal = sums.amount_sum,
                        total = sums.amount_sum,
                        updated_at = %s
                    FROM sums
                    WHERE id = %s
                    """,
                    (int(invoice_id), utc_now_iso(), int(invoice_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_invoice_status(
        self,
        invoice_id: int,
        status: str,
        *,
        updated_at: str | None = None,
        paid_at: str | None = None,
        external_reference: str | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE invoices
                    SET status = %s,
                        updated_at = %s,
                        paid_at = COALESCE(%s, paid_at),
                        external_reference = COALESCE(%s, external_reference)
                    WHERE id = %s
                    """,
                    (str(status), updated_at or utc_now_iso(), paid_at, external_reference, int(invoice_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def get_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM invoices WHERE id = %s LIMIT 1", (int(invoice_id),))
                row = cur.fetchone()
        return dict(row) if row else None

    def get_last_invoice(self, tenant_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM invoices
                    WHERE tenant_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def count_open_invoices(self, tenant_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM invoices
                    WHERE tenant_id = %s
                      AND status IN ('draft', 'open', 'uncollectible')
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return int((row or {}).get("cnt") or 0)
