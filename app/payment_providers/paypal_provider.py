from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult, PaymentStatusResult, PaymentWebhookResult


class PaypalProvider:
    provider_name = "paypal"

    def is_available(self) -> bool:
        return bool(settings.payment_enabled and settings.paypal_enabled and settings.paypal_client_id and settings.paypal_client_secret)

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        if not self.is_available():
            return PaymentProviderResult(provider=self.provider_name, status="failed", error_text="paypal_not_configured")
        invoice_id = int(invoice.get("id") or 0)
        tenant_id = int(tenant.get("id") or 0)
        external_id = f"paypal-{tenant_id}-{invoice_id}"
        checkout_url = f"https://www.paypal.com/checkoutnow?token={external_id}"
        payload = {"mode": settings.paypal_env, "return_url": return_url, "sandbox_ready": True}
        return PaymentProviderResult(
            provider=self.provider_name,
            status="pending",
            external_payment_id=external_id,
            external_checkout_url=checkout_url,
            payload=payload,
            user_message_ru="🅿️ PayPal: перейдите по ссылке для оплаты.",
            user_message_en="🅿️ PayPal: open the link to complete payment.",
        )

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult:
        event = self._safe_json(body)
        event_type = str(event.get("event_type") or "").upper()
        ext_id = str((event.get("resource") or {}).get("invoice_id") or event.get("invoice_id") or "")
        amount = float((((event.get("resource") or {}).get("amount") or {}).get("value") or 0) or 0)
        currency = str((((event.get("resource") or {}).get("amount") or {}).get("currency_code") or "USD")).upper()
        status = "paid" if event_type in {"CHECKOUT.ORDER.APPROVED", "PAYMENT.CAPTURE.COMPLETED"} else "failed"
        return PaymentWebhookResult(
            provider=self.provider_name,
            handled=bool(ext_id),
            external_payment_id=ext_id or None,
            status=status,
            amount=amount,
            currency=currency,
            payload=event,
        )

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult:
        return PaymentStatusResult(provider=self.provider_name, status=str(payment_intent.get("status") or "pending"), external_payment_id=payment_intent.get("external_payment_id"))

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool:
        _ = payment_intent
        return True

    @staticmethod
    def verify_webhook_signature(headers: dict[str, Any], body: str) -> bool:
        if not settings.paypal_webhook_id:
            return True
        signature = str(headers.get("X-Paypal-Signature") or "")
        if not signature:
            return False
        expected = hmac.new(settings.paypal_webhook_id.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)

    @staticmethod
    def _safe_json(body: str) -> dict[str, Any]:
        try:
            raw = json.loads(body)
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}
