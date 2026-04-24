from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult, PaymentStatusResult, PaymentWebhookResult


class TributeProvider:
    provider_name = "tribute"

    def is_available(self) -> bool:
        return bool(settings.payment_enabled and settings.tribute_enabled)

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        ext_id = f"tribute-{tenant.get('id')}-{invoice.get('id')}"
        checkout = return_url or "https://tribute.tg/"
        return PaymentProviderResult(
            provider=self.provider_name,
            status="pending",
            external_payment_id=ext_id,
            external_checkout_url=checkout,
            payload={"note": "placeholder_checkout", "invoice_id": invoice.get("id")},
            user_message_ru="🎁 Tribute: используйте внешнюю ссылку для оплаты.",
            user_message_en="🎁 Tribute: use external checkout link.",
        )

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult:
        if not self.verify_signature(headers, body):
            return PaymentWebhookResult(provider=self.provider_name, handled=False, error_text="invalid_signature")
        data = self._safe_json(body)
        ext_id = str(data.get("external_payment_id") or data.get("payment_id") or "")
        return PaymentWebhookResult(
            provider=self.provider_name,
            handled=bool(ext_id),
            external_payment_id=ext_id or None,
            status="paid" if str(data.get("status") or "").lower() in {"paid", "confirmed", "success"} else "failed",
            amount=float(data.get("amount") or 0),
            currency=str(data.get("currency") or "USD").upper(),
            payload=data,
        )

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult:
        return PaymentStatusResult(provider=self.provider_name, status=str(payment_intent.get("status") or "pending"))

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool:
        _ = payment_intent
        return True

    @staticmethod
    def verify_signature(headers: dict[str, Any], body: str) -> bool:
        if not settings.tribute_webhook_secret:
            return True
        signature = str(headers.get("X-Tribute-Signature") or "")
        if not signature:
            return False
        expected = hmac.new(settings.tribute_webhook_secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)

    @staticmethod
    def _safe_json(body: str) -> dict[str, Any]:
        try:
            raw = json.loads(body)
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}
