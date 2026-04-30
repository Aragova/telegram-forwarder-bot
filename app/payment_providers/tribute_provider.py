from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult, PaymentStatusResult, PaymentWebhookResult
from app.payment_providers.tribute_client import TributeAPIError, TributeClient

logger = logging.getLogger("forwarder.payments.tribute")


class TributeProvider:
    provider_name = "tribute"

    def is_available(self) -> bool:
        return bool(settings.payment_enabled and settings.tribute_enabled and settings.tribute_api_key)

    def create_payment(self, invoice: dict[str, Any], tenant: dict[str, Any], return_url: str | None = None) -> PaymentProviderResult:
        if not settings.tribute_api_key:
            return PaymentProviderResult(provider=self.provider_name, status="failed", error_text="tribute_api_key_missing", user_message_ru="Способ оплаты Tribute временно недоступен.")
        tenant_id = int(tenant.get("id") or 0)
        invoice_id = int(invoice.get("id") or 0)
        amount_major = float(invoice.get("total") or 0)
        amount_minor = int(round(amount_major * 100))  # Tribute Shop API использует minor units (например, $9 => 900)
        currency = str(invoice.get("currency") or "USD").lower()
        customer_id = f"tenant:{tenant_id}:invoice:{invoice_id}"
        comment = f"vimi:tenant:{tenant_id}:invoice:{invoice_id}:provider:tribute"
        payload = {
            "amount": amount_minor,
            "currency": currency,
            "title": f"ViMi {str(invoice.get('tariff_code') or 'BASIC').upper()}",
            "description": "Доступ к ViMi Bot",
            "customerId": customer_id,
            "comment": comment,
            "period": "onetime",
            "successUrl": settings.tribute_success_url,
            "failUrl": settings.tribute_fail_url,
        }
        logger.info("TRIBUTE_CREATE_ORDER_START tenant_id=%s invoice_id=%s amount=%s currency=%s provider=tribute", tenant_id, invoice_id, amount_major, str(invoice.get("currency") or "USD").upper())
        try:
            raw = TributeClient().create_shop_order(payload)
        except TributeAPIError as exc:
            logger.warning("TRIBUTE_CREATE_ORDER_FAILED tenant_id=%s invoice_id=%s status_code=%s error=%s", tenant_id, invoice_id, exc.status_code, str(exc))
            return PaymentProviderResult(provider=self.provider_name, status="failed", payload={"error": "tribute_create_order_failed", "invoice_id": invoice_id, "tenant_id": tenant_id}, error_text="tribute_create_order_failed", user_message_ru="Не удалось создать ссылку Tribute. Попробуйте другой способ оплаты или напишите в поддержку.")
        order_uuid = self._extract_order_uuid(raw)
        payment_url = str(raw.get("paymentUrl") or "")
        webapp_payment_url = str(raw.get("webappPaymentUrl") or "")
        checkout_url = webapp_payment_url or payment_url
        if not order_uuid or not checkout_url:
            return PaymentProviderResult(provider=self.provider_name, status="failed", payload={"error": "tribute_create_order_failed", "invoice_id": invoice_id, "tenant_id": tenant_id}, error_text="tribute_create_order_failed", user_message_ru="Не удалось создать ссылку Tribute. Попробуйте другой способ оплаты или напишите в поддержку.")
        logger.info("TRIBUTE_CREATE_ORDER_OK tenant_id=%s invoice_id=%s order_uuid=%s checkout_url_present=%s webapp_url_present=%s", tenant_id, invoice_id, order_uuid, bool(checkout_url), bool(webapp_payment_url))
        return PaymentProviderResult(provider=self.provider_name, status="pending", external_payment_id=order_uuid, external_checkout_url=checkout_url, payload={"order_uuid": order_uuid, "payment_url": payment_url, "webapp_payment_url": webapp_payment_url, "customer_id": customer_id, "comment": comment, "raw_create_response": raw}, user_message_ru="🎁 Оплатите через Tribute. После оплаты доступ включится автоматически.", user_message_en="🎁 Pay via Tribute. Access will be activated automatically after payment.")

    def handle_webhook(self, headers: dict[str, Any], body: str) -> PaymentWebhookResult:
        if not self.verify_signature(headers, body):
            logger.warning("TRIBUTE_WEBHOOK_INVALID_SIGNATURE")
            return PaymentWebhookResult(provider=self.provider_name, handled=False, error_text="invalid_signature")
        data = self._safe_json(body)
        if not data:
            return PaymentWebhookResult(provider=self.provider_name, handled=False, error_text="bad_json")
        event_name = str(data.get("name") or "")
        if event_name != "shop_order":
            return PaymentWebhookResult(provider=self.provider_name, handled=False, error_text="unsupported_event", payload=data)
        payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
        order_uuid = self._extract_order_uuid(payload)
        status_raw = str(payload.get("status") or "").lower()
        amount_raw = payload.get("amount")
        normalized_amount = None
        if amount_raw is not None:
            # Для Tribute webhook amount приходит в minor units, переводим в major units для сравнения с payment_intents.amount
            normalized_amount = float(amount_raw) / 100.0
        logger.info("TRIBUTE_WEBHOOK_RECEIVED event_name=%s order_uuid=%s status=%s", event_name, order_uuid, status_raw)
        return PaymentWebhookResult(provider=self.provider_name, handled=bool(order_uuid), external_payment_id=order_uuid or None, status="paid" if status_raw in {"paid", "success", "confirmed"} else "failed", amount=normalized_amount, currency=str(payload.get("currency") or "USD").upper(), payload=data)

    def check_payment_status(self, payment_intent: dict[str, Any]) -> PaymentStatusResult:
        return PaymentStatusResult(provider=self.provider_name, status=str(payment_intent.get("status") or "pending"))

    def cancel_payment(self, payment_intent: dict[str, Any]) -> bool:
        _ = payment_intent
        return True

    @staticmethod
    def verify_signature(headers: dict[str, Any], body: str) -> bool:
        signature = ""
        for key, value in headers.items():
            if str(key).lower() == "trbt-signature":
                signature = str(value or "")
                break
        api_key = str(settings.tribute_api_key or "")
        if not signature or not api_key:
            return False
        expected = hmac.new(api_key.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)

    @staticmethod
    def _extract_order_uuid(payload: dict[str, Any]) -> str:
        return str(payload.get("uuid") or payload.get("orderUuid") or payload.get("order_uuid") or payload.get("id") or "")

    @staticmethod
    def _safe_json(body: str) -> dict[str, Any]:
        try:
            raw = json.loads(body)
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}
