from __future__ import annotations

from typing import Any

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult, PaymentStatusResult, PaymentWebhookResult


class TelegramPaymentsProvider:
    provider_name = "telegram_payments"

    def is_available(self) -> bool:
        return bool(settings.payment_enabled and settings.telegram_payments_enabled and settings.telegram_payment_provider_token)

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        _ = tenant
        _ = return_url
        external_id = f"tg-pay-{invoice.get('id')}"
        return PaymentProviderResult(
            provider=self.provider_name,
            status="pending",
            external_payment_id=external_id,
            payload={"invoice_id": invoice.get("id"), "provider_token_configured": bool(settings.telegram_payment_provider_token)},
            user_message_ru="💳 Telegram Payments: ожидается подтверждение в Telegram.",
            user_message_en="💳 Telegram Payments: waiting for Telegram confirmation.",
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
