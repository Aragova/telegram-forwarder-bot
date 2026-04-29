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



    def get_billing_exchange_rates(self) -> dict[str, float]:
        currencies = ("RUB", "EUR", "UAH")
        rates: dict[str, float] = {}
        with self.connect() as conn:
            with conn.cursor() as cur:
                for currency in currencies:
                    cur.execute(
                        """
                        SELECT metadata_json
                        FROM billing_events
                        WHERE event_type = 'billing_rate_updated'
                          AND currency = %s
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (currency,),
                    )
                    row = cur.fetchone()
                    data = self.safe_json_loads((row or {}).get("metadata_json"), {})
                    value = data.get("new_value")
                    if value is not None:
                        try:
                            rates[f"USD_TO_{currency}"] = float(value)
                        except Exception:
                            pass
        return rates

    def set_billing_exchange_rate(self, *, currency: str, new_value: float, admin_id: int | None = None) -> bool:
        code = str(currency or "").upper()
        if code not in {"RUB", "EUR", "UAH"}:
            return False
        old_value = self.get_billing_exchange_rates().get(f"USD_TO_{code}")
        self.create_billing_event(
            1,
            "billing_rate_updated",
            event_source="admin_system",
            currency=code,
            metadata={"old_value": old_value, "new_value": float(new_value), "admin_id": admin_id, "currency": code},
        )
        return True

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

    def list_invoices_for_tenant(self, tenant_id: int, limit: int = 10) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM invoices
                    WHERE tenant_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (int(tenant_id), int(limit)),
                )
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

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

    def get_invoice_for_period(self, tenant_id: int, period_start: str, period_end: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM invoices
                    WHERE tenant_id = %s
                      AND period_start = %s
                      AND period_end = %s
                      AND status != 'void'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(tenant_id), str(period_start), str(period_end)),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def list_invoice_items(self, invoice_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM invoice_items
                    WHERE invoice_id = %s
                    ORDER BY id ASC
                    """,
                    (int(invoice_id),),
                )
                rows = cur.fetchall() or []
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata_json"] = self.safe_json_loads(item.get("metadata_json"), {})
            result.append(item)
        return result

    def count_invoices_by_status(self, status: str) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM invoices WHERE status = %s", (str(status),))
                row = cur.fetchone()
        return int((row or {}).get("cnt") or 0)

    def get_billing_periods_due(self, now_iso: str) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM subscriptions
                    WHERE status IN ('active', 'trial', 'grace')
                      AND current_period_end IS NOT NULL
                      AND current_period_end <= %s
                    """,
                    (str(now_iso),),
                )
                row = cur.fetchone()
        return int((row or {}).get("cnt") or 0)

    def count_tenants_with_overage_current_period(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH usage_period AS (
                        SELECT
                            s.tenant_id,
                            COALESCE(SUM(u.jobs_count), 0) AS jobs_count,
                            COALESCE(SUM(u.video_count), 0) AS video_count,
                            COALESCE(MAX(u.storage_used_mb), 0) AS storage_used_mb,
                            p.max_jobs_per_day,
                            p.max_video_per_day,
                            p.max_storage_mb
                        FROM subscriptions s
                        JOIN plans p ON p.id = s.plan_id
                        LEFT JOIN usage_stats u
                          ON u.tenant_id = s.tenant_id
                         AND s.current_period_start IS NOT NULL
                         AND s.current_period_end IS NOT NULL
                         AND u.date >= s.current_period_start
                         AND u.date <= s.current_period_end
                        WHERE s.status IN ('active', 'trial', 'grace')
                        GROUP BY s.tenant_id, p.max_jobs_per_day, p.max_video_per_day, p.max_storage_mb
                    )
                    SELECT COUNT(*) AS cnt
                    FROM usage_period
                    WHERE (max_jobs_per_day > 0 AND jobs_count > max_jobs_per_day)
                       OR (max_video_per_day > 0 AND video_count > max_video_per_day)
                       OR (max_storage_mb > 0 AND storage_used_mb > max_storage_mb)
                    """
                )
                row = cur.fetchone()
        return int((row or {}).get("cnt") or 0)
