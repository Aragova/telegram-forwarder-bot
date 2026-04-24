from __future__ import annotations

import json
from typing import Any

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult, PaymentStatusResult, PaymentWebhookResult


class ManualTransferProvider:
    def __init__(self, provider_name: str, text_ru: str, text_en: str, enabled: bool = True) -> None:
        self.provider_name = provider_name
        self._text_ru = text_ru
        self._text_en = text_en
        self._enabled = bool(enabled)

    def is_available(self) -> bool:
        return self._enabled and bool(settings.payment_enabled)

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        _ = return_url
        return PaymentProviderResult(
            provider=self.provider_name,
            status="waiting_confirmation",
            payload={"instruction_ru": self._text_ru, "instruction_en": self._text_en, "invoice_id": invoice.get("id")},
            user_message_ru=self._text_ru,
            user_message_en=self._text_en,
        )

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult:
        _ = headers
        _ = body
        return PaymentWebhookResult(provider=self.provider_name, handled=False, error_text="manual_provider_no_webhook")

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult:
        return PaymentStatusResult(provider=self.provider_name, status=str(payment_intent.get("status") or "waiting_confirmation"))

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool:
        _ = payment_intent
        return True

    @staticmethod
    def parse_manual_payload(body: str) -> dict[str, Any]:
        try:
            return json.loads(body)
        except Exception:
            return {}
