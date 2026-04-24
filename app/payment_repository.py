from __future__ import annotations

from typing import Any

from app.repository_models import utc_now_iso
from app.repository_split_base import RepositorySplitBase


class PaymentRepository(RepositorySplitBase):
    def create_payment_intent(
        self,
        *,
        tenant_id: int,
        invoice_id: int,
        provider: str,
        status: str,
        amount: float,
        currency: str,
        expires_at: str | None = None,
    ) -> int | None:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            row_id = self.fetch_inserted_id(
                conn,
                """
                INSERT INTO payment_intents(
                    tenant_id, invoice_id, provider, status, amount, currency,
                    created_at, updated_at, expires_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(tenant_id),
                    int(invoice_id),
                    str(provider),
                    str(status),
                    float(amount),
                    str(currency).upper(),
                    now_iso,
                    now_iso,
                    expires_at,
                ),
            )
            conn.commit()
            return row_id

    def get_payment_intent(self, payment_intent_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payment_intents WHERE id = %s LIMIT 1", (int(payment_intent_id),))
                row = cur.fetchone()
        return self._norm_row(row)

    def get_payment_intent_by_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM payment_intents
                    WHERE invoice_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(invoice_id),),
                )
                row = cur.fetchone()
        return self._norm_row(row)

    def get_payment_intent_by_external_id(self, external_payment_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM payment_intents
                    WHERE external_payment_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (str(external_payment_id),),
                )
                row = cur.fetchone()
        return self._norm_row(row)

    def update_payment_intent_status(self, payment_intent_id: int, status: str, *, error_text: str | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE payment_intents
                    SET status = %s,
                        error_text = COALESCE(%s, error_text),
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (str(status), error_text, utc_now_iso(), int(payment_intent_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def attach_checkout_url(self, payment_intent_id: int, checkout_url: str, *, external_payment_id: str | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE payment_intents
                    SET external_checkout_url = %s,
                        external_payment_id = COALESCE(%s, external_payment_id),
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (str(checkout_url), external_payment_id, utc_now_iso(), int(payment_intent_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def attach_provider_payload(self, payment_intent_id: int, payload: dict[str, Any]) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE payment_intents
                    SET provider_payload_json = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (self.json_dumps(payload or {}), utc_now_iso(), int(payment_intent_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mark_payment_paid(self, payment_intent_id: int, *, confirmation_payload: dict[str, Any] | None = None) -> bool:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE payment_intents
                    SET status = 'paid',
                        confirmation_payload_json = COALESCE(%s, confirmation_payload_json),
                        paid_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (self.json_dumps(confirmation_payload), now_iso, now_iso, int(payment_intent_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mark_payment_failed(self, payment_intent_id: int, error_text: str, *, payload: dict[str, Any] | None = None) -> bool:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE payment_intents
                    SET status = 'failed',
                        failed_at = %s,
                        error_text = %s,
                        confirmation_payload_json = COALESCE(%s, confirmation_payload_json),
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (now_iso, str(error_text), self.json_dumps(payload), now_iso, int(payment_intent_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def list_payment_intents_for_tenant(self, tenant_id: int, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM payment_intents
                    WHERE tenant_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (int(tenant_id), int(limit)),
                )
                rows = cur.fetchall() or []
        return [self._norm_row(row) for row in rows]

    def attach_confirmation_payload(self, payment_intent_id: int, payload: dict[str, Any]) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE payment_intents
                    SET confirmation_payload_json = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (self.json_dumps(payload or {}), utc_now_iso(), int(payment_intent_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def _norm_row(self, row: Any) -> dict[str, Any] | None:
        if not row:
            return None
        item = dict(row)
        item["provider_payload_json"] = self.safe_json_loads(item.get("provider_payload_json"), {})
        item["confirmation_payload_json"] = self.safe_json_loads(item.get("confirmation_payload_json"), {})
        return item
