from __future__ import annotations

from typing import Any

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult, PaymentStatusResult, PaymentWebhookResult


class CryptoManualProvider:
    provider_name = "crypto_manual"

    def is_available(self) -> bool:
        return bool(settings.payment_enabled and settings.crypto_manual_enabled)

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        _ = return_url
        _ = tenant
        ru_lines = [
            "🪙 Оплата криптовалютой (ручная проверка)",
            "USDT TRC20: " + (settings.crypto_usdt_trc20_address or "не задан"),
            "USDT TON: " + (settings.crypto_usdt_ton_address or "не задан"),
            "BTC: " + (settings.crypto_btc_address or "не задан"),
            "После перевода отправьте tx hash.",
        ]
        en_lines = [
            "🪙 Cryptocurrency payment (manual verification)",
            "USDT TRC20: " + (settings.crypto_usdt_trc20_address or "not set"),
            "USDT TON: " + (settings.crypto_usdt_ton_address or "not set"),
            "BTC: " + (settings.crypto_btc_address or "not set"),
            "After transfer, send tx hash.",
        ]
        return PaymentProviderResult(
            provider=self.provider_name,
            status="waiting_confirmation",
            payload={"invoice_id": invoice.get("id"), "addresses": {"usdt_trc20": settings.crypto_usdt_trc20_address, "usdt_ton": settings.crypto_usdt_ton_address, "btc": settings.crypto_btc_address}},
            user_message_ru="\n".join(ru_lines),
            user_message_en="\n".join(en_lines),
        )

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult:
        _ = headers
        _ = body
        return PaymentWebhookResult(provider=self.provider_name, handled=False, error_text="manual_crypto_no_webhook")

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult:
        return PaymentStatusResult(provider=self.provider_name, status=str(payment_intent.get("status") or "waiting_confirmation"))

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool:
        _ = payment_intent
        return True
