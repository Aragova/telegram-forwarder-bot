from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class PaymentSyncResult:
    external_reference: str
    status: str
    raw_payload: dict[str, Any]


class PaymentProviderProtocol(Protocol):
    def create_checkout_session(self, tenant_id: int, invoice_id: int, *, return_url: str) -> dict[str, Any]: ...

    def create_subscription_charge(self, tenant_id: int, subscription_id: int, amount: float, currency: str) -> dict[str, Any]: ...

    def mark_external_payment(self, external_reference: str, *, status: str, payload: dict[str, Any] | None = None) -> PaymentSyncResult: ...

    def sync_payment_status(self, external_reference: str) -> PaymentSyncResult: ...


class NoopPaymentProvider:
    """Временный провайдер без реальной интеграции оплаты."""

    def create_checkout_session(self, tenant_id: int, invoice_id: int, *, return_url: str) -> dict[str, Any]:
        return {
            "provider": "noop",
            "tenant_id": int(tenant_id),
            "invoice_id": int(invoice_id),
            "return_url": str(return_url),
            "external_reference": f"noop-checkout-{tenant_id}-{invoice_id}",
            "status": "not_implemented",
        }

    def create_subscription_charge(self, tenant_id: int, subscription_id: int, amount: float, currency: str) -> dict[str, Any]:
        return {
            "provider": "noop",
            "tenant_id": int(tenant_id),
            "subscription_id": int(subscription_id),
            "amount": float(amount),
            "currency": str(currency).upper(),
            "external_reference": f"noop-charge-{tenant_id}-{subscription_id}",
            "status": "not_implemented",
        }

    def mark_external_payment(self, external_reference: str, *, status: str, payload: dict[str, Any] | None = None) -> PaymentSyncResult:
        return PaymentSyncResult(
            external_reference=str(external_reference),
            status=str(status),
            raw_payload=payload or {},
        )

    def sync_payment_status(self, external_reference: str) -> PaymentSyncResult:
        return PaymentSyncResult(
            external_reference=str(external_reference),
            status="unknown",
            raw_payload={"provider": "noop"},
        )
