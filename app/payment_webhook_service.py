from __future__ import annotations

from typing import Any

from app.payment_service import PaymentService


class PaymentWebhookService:
    def __init__(self, repo) -> None:
        self._payment_service = PaymentService(repo)

    def handle_paypal_webhook(self, headers: dict[str, Any], body: str) -> dict[str, Any]:
        return self._payment_service.handle_provider_webhook("paypal", headers, body)

    def handle_tribute_webhook(self, headers: dict[str, Any], body: str) -> dict[str, Any]:
        return self._payment_service.handle_provider_webhook("tribute", headers, body)

    def handle_lava_top_webhook(self, headers: dict[str, Any], body: str) -> dict[str, Any]:
        return self._payment_service.handle_provider_webhook("lava_top", headers, body)

    def handle_telegram_payment_update(self, update: dict[str, Any]) -> dict[str, Any]:
        external_id = str(update.get("invoice_payload") or update.get("provider_payment_charge_id") or "")
        if not external_id:
            return {"ok": False, "error": "external_payment_id_missing"}
        return self._payment_service.handle_provider_webhook(
            "telegram_payments",
            headers={},
            body=str({"external_payment_id": external_id, "status": "paid", "amount": update.get("total_amount"), "currency": update.get("currency")}),
        )
