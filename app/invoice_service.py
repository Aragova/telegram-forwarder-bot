from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class InvoiceService:
    def __init__(self, repo) -> None:
        self._repo = repo

    def create_draft_invoice(
        self,
        tenant_id: int,
        subscription_id: int,
        period_start: str,
        period_end: str,
        *,
        currency: str = "USD",
        due_at: str | None = None,
    ) -> int | None:
        if not hasattr(self._repo, "create_invoice"):
            return None
        invoice_id = self._repo.create_invoice(
            tenant_id=int(tenant_id),
            subscription_id=int(subscription_id),
            period_start=str(period_start),
            period_end=str(period_end),
            status="draft",
            currency=str(currency).upper(),
            due_at=due_at,
        )
        if invoice_id and hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(tenant_id),
                "invoice_created",
                event_source="invoice_service",
                metadata={"invoice_id": int(invoice_id), "status": "draft"},
            )
        return invoice_id

    def add_invoice_item(
        self,
        invoice_id: int,
        *,
        item_type: str,
        description: str,
        quantity: int,
        unit_price: float,
        metadata: dict[str, Any] | None = None,
    ) -> int | None:
        if not hasattr(self._repo, "add_invoice_item"):
            return None
        qty = max(int(quantity), 0)
        price = float(unit_price)
        amount = round(qty * price, 2)
        item_id = self._repo.add_invoice_item(
            int(invoice_id),
            item_type=str(item_type),
            description=str(description),
            quantity=qty,
            unit_price=price,
            amount=amount,
            metadata=metadata or {},
        )
        if item_id and hasattr(self._repo, "recalculate_invoice_totals"):
            self._repo.recalculate_invoice_totals(int(invoice_id))
        return item_id

    def finalize_invoice(self, invoice_id: int) -> bool:
        if not hasattr(self._repo, "set_invoice_status"):
            return False
        return bool(self._repo.set_invoice_status(int(invoice_id), "open", updated_at=datetime.now(timezone.utc).isoformat()))

    def mark_invoice_paid(self, invoice_id: int, *, external_reference: str | None = None) -> bool:
        if not hasattr(self._repo, "set_invoice_status"):
            return False
        ok = bool(
            self._repo.set_invoice_status(
                int(invoice_id),
                "paid",
                updated_at=datetime.now(timezone.utc).isoformat(),
                paid_at=datetime.now(timezone.utc).isoformat(),
                external_reference=external_reference,
            )
        )
        if not ok:
            return False
        if hasattr(self._repo, "get_invoice") and hasattr(self._repo, "create_billing_event"):
            invoice = self._repo.get_invoice(int(invoice_id)) or {}
            self._repo.create_billing_event(
                int(invoice.get("tenant_id") or 0),
                "invoice_marked_paid",
                event_source="invoice_service",
                metadata={"invoice_id": int(invoice_id), "external_reference": external_reference},
            )
        return True

    def mark_invoice_void(self, invoice_id: int, *, reason: str = "") -> bool:
        if not hasattr(self._repo, "set_invoice_status"):
            return False
        ok = bool(self._repo.set_invoice_status(int(invoice_id), "void", updated_at=datetime.now(timezone.utc).isoformat()))
        if not ok:
            return False
        if hasattr(self._repo, "get_invoice") and hasattr(self._repo, "create_billing_event"):
            invoice = self._repo.get_invoice(int(invoice_id)) or {}
            self._repo.create_billing_event(
                int(invoice.get("tenant_id") or 0),
                "invoice_marked_void",
                event_source="invoice_service",
                metadata={"invoice_id": int(invoice_id), "reason": str(reason)},
            )
        return True
