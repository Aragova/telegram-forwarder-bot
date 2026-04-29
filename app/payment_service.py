from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from app.config import settings
from app.invoice_service import InvoiceService
from app.payment_providers import (
    CryptoManualProvider,
    LavaTopProvider,
    ManualTransferProvider,
    PaypalProvider,
    TelegramPaymentsProvider,
    TelegramStarsProvider,
    TributeProvider,
)
from app.subscription_service import SubscriptionService

MANUAL_PROVIDER_TYPES = {"manual_bank_card", "card_provider", "sbp_provider", "crypto_manual"}


class PaymentService:
    def __init__(self, repo) -> None:
        self._repo = repo
        self._invoice_service = InvoiceService(repo)
        self._subscription_service = SubscriptionService(repo)
        self._providers = self._build_providers()

    def create_payment_for_invoice(self, invoice_id: int, provider: str, *, attempt_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        invoice = self._repo.get_invoice(int(invoice_id)) if hasattr(self._repo, "get_invoice") else None
        if not invoice:
            return {"ok": False, "error": "invoice_not_found"}
        if str(invoice.get("status") or "") == "paid":
            return {"ok": False, "error": "invoice_already_paid"}
        tenant_id = int(invoice.get("tenant_id") or 0)
        tenant = self._repo.get_tenant_by_id(tenant_id) if hasattr(self._repo, "get_tenant_by_id") else {"id": tenant_id}
        sub = self._subscription_service.get_active_subscription(tenant_id) or {}
        if str(sub.get("plan_name") or "").upper() == "OWNER":
            return {"ok": False, "error": "owner_payment_not_required"}

        provider_key = str(provider)
        adapter = self._providers.get(provider_key)
        if not adapter or not adapter.is_available():
            return {"ok": False, "error": "provider_unavailable"}

        amount = float(invoice.get("total") or 0)
        currency = str(invoice.get("currency") or "USD").upper()
        active_intent = self._repo.get_active_payment_intent_for_invoice_provider(int(invoice_id), provider_key) if hasattr(self._repo, "get_active_payment_intent_for_invoice_provider") else None
        if isinstance(active_intent, dict) and str(active_intent.get("status") or "") in {"created", "checkout_opened", "pending", "waiting_confirmation"}:
            return {
                "ok": True,
                "payment_intent_id": int(active_intent.get("id") or 0),
                "provider": provider_key,
                "status": str(active_intent.get("status") or "created"),
                "checkout_url": active_intent.get("external_checkout_url"),
                "idempotent": True,
            }

        intent_status = "waiting_confirmation" if provider_key in MANUAL_PROVIDER_TYPES else "created"
        intent_id = self._repo.create_payment_intent(
            tenant_id=tenant_id,
            invoice_id=int(invoice_id),
            provider=provider_key,
            status=intent_status,
            amount=amount,
            currency=currency,
        )
        if not intent_id:
            return {"ok": False, "error": "create_payment_intent_failed"}

        result = adapter.create_payment(invoice=invoice, tenant=tenant or {"id": tenant_id})
        payload = asdict(result)
        if attempt_id:
            payload["attempt_id"] = str(attempt_id)
        if idempotency_key:
            payload["idempotency_key"] = str(idempotency_key)
        self._repo.attach_provider_payload(int(intent_id), payload)
        normalized_status = str(result.status or intent_status or "created")
        if provider_key in MANUAL_PROVIDER_TYPES:
            normalized_status = "waiting_confirmation"
        elif result.external_checkout_url:
            normalized_status = "checkout_opened"
        elif normalized_status in {"failed", "error", "canceled"}:
            normalized_status = "provider_failed"
        if result.external_checkout_url:
            self._repo.attach_checkout_url(int(intent_id), result.external_checkout_url, external_payment_id=result.external_payment_id)
        elif result.external_payment_id:
            self._repo.attach_provider_payload(int(intent_id), {**asdict(result), "external_payment_id": result.external_payment_id})
        self._repo.update_payment_intent_status(int(intent_id), normalized_status, error_text=result.error_text)

        return {"ok": True, "payment_intent_id": int(intent_id), "provider": provider_key, "status": normalized_status, "checkout_url": result.external_checkout_url, "message_ru": result.user_message_ru, "message_en": result.user_message_en, "payload": result.payload, "idempotent": False, "error_text": result.error_text}

    def get_available_payment_methods(self, tenant_id: int, invoice_id: int) -> list[dict[str, Any]]:
        _ = tenant_id
        invoice = self._repo.get_invoice(int(invoice_id)) if hasattr(self._repo, "get_invoice") else None
        if not invoice:
            return []
        if str(invoice.get("status") or "") == "paid":
            return []
        methods = []
        for key, adapter in self._providers.items():
            if adapter.is_available():
                methods.append({"provider": key})
        allowed = {v.strip() for v in settings.payment_allowed_providers if v.strip()}
        if allowed:
            methods = [m for m in methods if m["provider"] in allowed]
        return methods

    def list_available_methods(self, invoice: dict[str, Any]) -> list[dict[str, Any]]:
        """Совместимость с существующими handler'ами оплаты."""
        if not invoice:
            return []
        tenant_id = int(invoice.get("tenant_id") or 0)
        invoice_id = int(invoice.get("id") or 0)
        if tenant_id <= 0 or invoice_id <= 0:
            return []
        return self.get_available_payment_methods(tenant_id, invoice_id)

    def start_payment(self, invoice: dict[str, Any], provider: str, user_id: int | None = None) -> dict[str, Any]:
        """Совместимость с существующими handler'ами оплаты."""
        _ = user_id
        if not invoice:
            return {"ok": False, "error": "invoice_not_found"}
        invoice_id = int(invoice.get("id") or 0)
        if invoice_id <= 0:
            return {"ok": False, "error": "invoice_not_found"}
        return self.create_payment_for_invoice(invoice_id, provider)

    def handle_provider_webhook(self, provider: str, headers: dict[str, Any], body: str) -> dict[str, Any]:
        adapter = self._providers.get(str(provider))
        if not adapter:
            return {"ok": False, "error": "provider_unknown"}
        webhook = adapter.handle_webhook(headers, body)
        if not webhook.handled:
            return {"ok": False, "error": webhook.error_text or "not_handled"}
        intent = self._repo.get_payment_intent_by_external_id(str(webhook.external_payment_id))
        if not intent:
            return {"ok": False, "error": "payment_intent_not_found"}
        if str(intent.get("status") or "") == "paid":
            return {"ok": True, "idempotent": True, "payment_intent_id": int(intent.get("id") or 0)}
        if webhook.amount is not None and abs(float(intent.get("amount") or 0) - float(webhook.amount)) > 1e-6:
            self._repo.mark_payment_failed(int(intent["id"]), "amount_mismatch", payload={"webhook": asdict(webhook)})
            return {"ok": False, "error": "amount_mismatch"}
        if webhook.currency and str(intent.get("currency") or "").upper() != str(webhook.currency).upper():
            self._repo.mark_payment_failed(int(intent["id"]), "currency_mismatch", payload={"webhook": asdict(webhook)})
            return {"ok": False, "error": "currency_mismatch"}
        if webhook.status == "paid":
            self._repo.mark_payment_paid(int(intent["id"]), confirmation_payload=asdict(webhook))
            self.activate_subscription_after_payment(int(intent["id"]))
            return {"ok": True, "payment_intent_id": int(intent["id"]), "status": "paid"}
        self._repo.mark_payment_failed(int(intent["id"]), webhook.error_text or "provider_failed", payload=asdict(webhook))
        return {"ok": False, "error": "provider_failed"}

    def confirm_manual_payment(self, payment_intent_id: int, admin_id: int, note: str) -> bool:
        intent = self._repo.get_payment_intent(int(payment_intent_id))
        if not intent:
            return False
        payload = dict(intent.get("confirmation_payload_json") or {})
        payload.update({"confirmed_by": int(admin_id), "note": str(note), "confirmed_at": datetime.now(timezone.utc).isoformat()})
        self._repo.mark_payment_paid(int(payment_intent_id), confirmation_payload=payload)
        self.activate_subscription_after_payment(int(payment_intent_id))
        return True

    def cancel_payment_intent(self, payment_intent_id: int) -> bool:
        intent = self._repo.get_payment_intent(int(payment_intent_id))
        if not intent:
            return False
        provider = self._providers.get(str(intent.get("provider") or ""))
        if provider and not provider.cancel_payment(intent):
            return False
        return bool(self._repo.update_payment_intent_status(int(payment_intent_id), "canceled"))

    def activate_subscription_after_payment(self, payment_intent_id: int) -> bool:
        intent = self._repo.get_payment_intent(int(payment_intent_id))
        if not intent or str(intent.get("status") or "") != "paid":
            return False
        invoice = self._repo.get_invoice(int(intent.get("invoice_id") or 0)) if hasattr(self._repo, "get_invoice") else None
        if not invoice:
            return False
        if str(invoice.get("status") or "") != "paid":
            self._invoice_service.mark_invoice_paid(int(invoice.get("id") or 0), external_reference=intent.get("external_payment_id"))
        tenant_id = int(invoice.get("tenant_id") or 0)
        items = self._repo.list_invoice_items(int(invoice.get("id") or 0)) if hasattr(self._repo, "list_invoice_items") else []
        target_plan = None
        for item in items:
            plan_name = str((item.get("metadata_json") or item.get("metadata") or {}).get("plan_name") or "").upper()
            if plan_name and plan_name not in {"FREE", "OWNER"}:
                target_plan = plan_name
                break
        if target_plan:
            self._subscription_service.change_plan(tenant_id, target_plan, changed_by="payment_service", reason="invoice_paid", effective_mode="immediate")
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(tenant_id, "payment_received", event_source="payment_service", amount=float(intent.get("amount") or 0), currency=str(intent.get("currency") or "USD"), metadata={"payment_intent_id": int(payment_intent_id), "invoice_id": int(invoice.get("id") or 0), "provider": intent.get("provider")})
            self._repo.create_billing_event(tenant_id, "subscription_activated", event_source="payment_service", metadata={"payment_intent_id": int(payment_intent_id), "invoice_id": int(invoice.get("id") or 0)})
        return True

    def save_manual_confirmation_payload(self, payment_intent_id: int, payload: dict[str, Any]) -> bool:
        return bool(self._repo.attach_confirmation_payload(int(payment_intent_id), payload))

    def _build_providers(self) -> dict[str, Any]:
        return {
            "telegram_stars": TelegramStarsProvider(),
            "telegram_payments": TelegramPaymentsProvider(),
            "paypal": PaypalProvider(),
            "card_provider": ManualTransferProvider("card_provider", settings.manual_card_text_ru, settings.manual_card_text_en, settings.manual_card_enabled),
            "manual_bank_card": ManualTransferProvider("manual_bank_card", settings.manual_card_text_ru, settings.manual_card_text_en, settings.manual_card_enabled),
            "sbp_provider": ManualTransferProvider("sbp_provider", settings.sbp_payment_text_ru, settings.sbp_payment_text_en, settings.sbp_manual_enabled),
            "crypto_manual": CryptoManualProvider(),
            "tribute": TributeProvider(),
            "lava_top": LavaTopProvider(),
        }
