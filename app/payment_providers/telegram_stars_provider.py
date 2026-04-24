from __future__ import annotations

from typing import Any

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult, PaymentStatusResult, PaymentWebhookResult


class TelegramStarsProvider:
    provider_name = "telegram_stars"

    def is_available(self) -> bool:
        return bool(settings.payment_enabled and settings.telegram_stars_enabled)

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        _ = tenant
        _ = return_url
        external_id = f"tg-stars-{invoice.get('id')}"
        return PaymentProviderResult(
            provider=self.provider_name,
            status="pending",
            external_payment_id=external_id,
            payload={"invoice_id": invoice.get("id"), "mode": "telegram_update"},
            user_message_ru="⭐ Оплата через Telegram Stars запущена.",
            user_message_en="⭐ Telegram Stars payment initialized.",
        )

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult:
        _ = headers
        _ = body
        return PaymentWebhookResult(provider=self.provider_name, handled=False)

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult:
        return PaymentStatusResult(provider=self.provider_name, status=str(payment_intent.get("status") or "pending"))

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool:
        _ = payment_intent
        return True
