from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(slots=True)
class PaymentProviderResult:
    provider: str
    status: str
    external_payment_id: str | None = None
    external_checkout_url: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    user_message_ru: str | None = None
    user_message_en: str | None = None
    error_text: str | None = None


@dataclass(slots=True)
class PaymentWebhookResult:
    provider: str
    handled: bool
    external_payment_id: str | None = None
    status: str | None = None
    amount: float | None = None
    currency: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    error_text: str | None = None


@dataclass(slots=True)
class PaymentStatusResult:
    provider: str
    status: str
    external_payment_id: str | None = None
    amount: float | None = None
    currency: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    error_text: str | None = None


class PaymentProviderProtocol(Protocol):
    provider_name: str

    def is_available(self) -> bool: ...

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult: ...

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult: ...

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult: ...

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool: ...


class NoopPaymentProvider:
    provider_name = "noop"

    def is_available(self) -> bool:
        return True

    # backward-compat API
    def create_checkout_session(self, tenant_id: int, invoice_id: int, *, return_url: str) -> dict[str, Any]:
        result = self.create_payment({"id": invoice_id}, {"id": tenant_id}, return_url=return_url)
        return {
            "provider": self.provider_name,
            "tenant_id": int(tenant_id),
            "invoice_id": int(invoice_id),
            "return_url": str(return_url),
            "external_reference": result.external_payment_id or f"noop-checkout-{tenant_id}-{invoice_id}",
            "status": "not_implemented",
        }

    def create_subscription_charge(self, tenant_id: int, subscription_id: int, amount: float, currency: str) -> dict[str, Any]:
        return {
            "provider": self.provider_name,
            "tenant_id": int(tenant_id),
            "subscription_id": int(subscription_id),
            "amount": float(amount),
            "currency": str(currency).upper(),
            "external_reference": f"noop-charge-{tenant_id}-{subscription_id}",
            "status": "not_implemented",
        }

    def mark_external_payment(self, external_reference: str, *, status: str, payload: dict[str, Any] | None = None):
        return PaymentStatusResult(
            provider=self.provider_name,
            status=str(status),
            external_payment_id=str(external_reference),
            payload=payload or {},
        )

    def sync_payment_status(self, external_reference: str):
        return PaymentStatusResult(
            provider=self.provider_name,
            status="unknown",
            external_payment_id=str(external_reference),
            payload={"provider": "noop"},
        )

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        return PaymentProviderResult(
            provider=self.provider_name,
            status="failed",
            payload={"reason": "not_configured", "invoice_id": invoice.get("id"), "tenant_id": tenant.get("id"), "return_url": return_url},
            error_text="Провайдер не настроен",
        )

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult:
        return PaymentWebhookResult(provider=self.provider_name, handled=False, error_text="Webhook не поддерживается")

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult:
        return PaymentStatusResult(provider=self.provider_name, status="unknown", payload={"payment_intent_id": payment_intent.get("id")})

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool:
        return True
