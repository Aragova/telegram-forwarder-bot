from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings
from app.payment_provider_protocol import PaymentProviderResult

from app.payment_providers.tribute_client import TributeAPIError, TributeClient
from app.payment_providers.tribute_provider import TributeProvider
from app.payment_service import PaymentService


class _FakeRepo:
    def __init__(self) -> None:
        self.tenants = {1: {"id": 1, "owner_admin_id": 100}}
        self.subscriptions = {1: {"id": 1, "tenant_id": 1, "plan_name": "PRO", "plan_id": 3, "status": "active", "price": 29}}
        self.invoices = {11: {"id": 11, "tenant_id": 1, "subscription_id": 1, "status": "open", "total": 29.0, "currency": "USD"}}
        self.invoice_items = {11: [{"id": 1, "invoice_id": 11, "description": "PRO", "metadata_json": {"plan_name": "PRO"}}]}
        self.payment_intents: dict[int, dict] = {}
        self.billing_events: list[dict] = []
        self._intent_seq = 0

    def get_invoice(self, invoice_id: int):
        return self.invoices.get(invoice_id)

    def get_tenant_by_id(self, tenant_id: int):
        return self.tenants.get(tenant_id)

    def get_active_subscription(self, tenant_id: int):
        return self.subscriptions.get(tenant_id)

    def list_invoice_items(self, invoice_id: int):
        return self.invoice_items.get(invoice_id, [])

    def create_payment_intent(self, **payload):
        self._intent_seq += 1
        self.payment_intents[self._intent_seq] = {"id": self._intent_seq, **payload, "provider_payload_json": {}, "confirmation_payload_json": {}, "external_payment_id": None, "status": payload.get("status", "created")}
        return self._intent_seq

    def get_payment_intent(self, payment_intent_id: int):
        return self.payment_intents.get(payment_intent_id)

    def get_payment_intent_by_invoice(self, invoice_id: int):
        rows = [p for p in self.payment_intents.values() if p["invoice_id"] == invoice_id]
        return rows[-1] if rows else None


    def get_active_payment_intent_for_invoice_provider(self, invoice_id: int, provider: str):
        rows = [
            p for p in self.payment_intents.values()
            if p["invoice_id"] == invoice_id and p.get("provider") == provider and str(p.get("status") or "") in {"created", "pending", "waiting_confirmation"}
        ]
        return rows[-1] if rows else None

    def get_payment_intent_by_external_id(self, external_payment_id: str):
        for row in self.payment_intents.values():
            if row.get("external_payment_id") == external_payment_id:
                return row
        return None

    def update_payment_intent_status(self, payment_intent_id: int, status: str, *, error_text=None):
        self.payment_intents[payment_intent_id]["status"] = status
        if error_text:
            self.payment_intents[payment_intent_id]["error_text"] = error_text
        return True

    def attach_checkout_url(self, payment_intent_id: int, checkout_url: str, *, external_payment_id=None):
        self.payment_intents[payment_intent_id]["external_checkout_url"] = checkout_url
        if external_payment_id:
            self.payment_intents[payment_intent_id]["external_payment_id"] = external_payment_id
        return True

    def attach_provider_payload(self, payment_intent_id: int, payload: dict):
        self.payment_intents[payment_intent_id]["provider_payload_json"] = payload
        if payload.get("external_payment_id"):
            self.payment_intents[payment_intent_id]["external_payment_id"] = payload["external_payment_id"]
        return True

    def attach_confirmation_payload(self, payment_intent_id: int, payload: dict):
        self.payment_intents[payment_intent_id]["confirmation_payload_json"] = payload
        return True

    def mark_payment_paid(self, payment_intent_id: int, *, confirmation_payload=None):
        self.payment_intents[payment_intent_id]["status"] = "paid"
        if confirmation_payload:
            self.payment_intents[payment_intent_id]["confirmation_payload_json"] = confirmation_payload
        return True

    def mark_payment_failed(self, payment_intent_id: int, error_text: str, *, payload=None):
        self.payment_intents[payment_intent_id]["status"] = "failed"
        self.payment_intents[payment_intent_id]["error_text"] = error_text
        return True

    def set_invoice_status(self, invoice_id: int, status: str, **kwargs):
        self.invoices[invoice_id]["status"] = status
        return True

    def create_billing_event(self, tenant_id: int, event_type: str, **kwargs):
        self.billing_events.append({"tenant_id": tenant_id, "event_type": event_type, **kwargs})
        return len(self.billing_events)

    def change_plan(self, tenant_id: int, new_plan_name: str):
        self.subscriptions[tenant_id]["plan_name"] = new_plan_name


def _service() -> tuple[_FakeRepo, PaymentService]:
    repo = _FakeRepo()
    service = PaymentService(repo)
    return repo, service


def test_available_providers_filtered_by_env():
    settings.payment_enabled = True
    settings.payment_allowed_providers_raw = "manual_bank_card,paypal"
    repo, service = _service()
    methods = service.get_available_payment_methods(1, 11)
    assert {m["provider"] for m in methods} <= {"manual_bank_card", "paypal"}


def test_create_payment_intent_for_invoice():
    settings.payment_enabled = True
    settings.manual_card_enabled = True
    repo, service = _service()
    out = service.create_payment_for_invoice(11, "manual_bank_card")
    assert out["ok"] is True
    assert repo.get_payment_intent(out["payment_intent_id"])["invoice_id"] == 11


def test_manual_card_waiting_confirmation():
    settings.payment_enabled = True
    settings.manual_card_enabled = True
    _repo, service = _service()
    out = service.create_payment_for_invoice(11, "manual_bank_card")
    assert out["status"] == "waiting_confirmation"


def test_manual_confirmation_marks_invoice_paid_and_events():
    settings.payment_enabled = True
    repo, service = _service()
    out = service.create_payment_for_invoice(11, "manual_bank_card")
    assert service.confirm_manual_payment(out["payment_intent_id"], admin_id=999, note="ok") is True
    assert repo.invoices[11]["status"] == "paid"
    assert any(e["event_type"] == "payment_received" for e in repo.billing_events)


def test_duplicate_webhook_is_idempotent():
    settings.payment_enabled = True
    settings.tribute_enabled = True
    settings.tribute_api_key = "secret"
    repo, service = _service()
    service._providers["tribute"].create_payment = lambda **kwargs: PaymentProviderResult(provider="tribute", status="pending", external_payment_id="x-1", external_checkout_url="https://example.test/pay")
    out = service.create_payment_for_invoice(11, "tribute")
    intent_id = out["payment_intent_id"]
    repo.payment_intents[intent_id]["external_payment_id"] = "x-1"
    body = '{"name":"shop_order","payload":{"uuid":"x-1","status":"paid","amount":2900,"currency":"usd"}}'
    import hashlib, hmac
    sig = hmac.new(b"secret", body.encode("utf-8"), hashlib.sha256).hexdigest()
    first = service.handle_provider_webhook("tribute", {"trbt-signature": sig}, body)
    second = service.handle_provider_webhook("tribute", {"trbt-signature": sig}, body)
    assert first["ok"] is True
    assert second.get("idempotent") is True


def test_paypal_disabled_without_credentials():
    settings.payment_enabled = True
    settings.paypal_enabled = True
    settings.paypal_client_id = ""
    settings.paypal_client_secret = ""
    _repo, service = _service()
    out = service.create_payment_for_invoice(11, "paypal")
    assert out["ok"] is False


def test_telegram_successful_payment_activates_invoice():
    settings.payment_enabled = True
    settings.telegram_payments_enabled = True
    settings.telegram_payment_provider_token = "token"
    repo, service = _service()
    out = service.create_payment_for_invoice(11, "telegram_payments")
    intent_id = out["payment_intent_id"]
    repo.mark_payment_paid(intent_id, confirmation_payload={"source": "telegram_successful_payment"})
    assert service.activate_subscription_after_payment(intent_id) is True
    assert repo.invoices[11]["status"] == "paid"


def test_crypto_tx_hash_saved():
    settings.payment_enabled = True
    settings.crypto_manual_enabled = True
    repo, service = _service()
    out = service.create_payment_for_invoice(11, "crypto_manual")
    assert service.save_manual_confirmation_payload(out["payment_intent_id"], {"tx_hash": "0xabc"})
    assert repo.get_payment_intent(out["payment_intent_id"])["confirmation_payload_json"]["tx_hash"] == "0xabc"


def test_owner_does_not_require_payment():
    settings.payment_enabled = True
    repo, service = _service()
    repo.subscriptions[1]["plan_name"] = "OWNER"
    out = service.create_payment_for_invoice(11, "manual_bank_card")
    assert out["error"] == "owner_payment_not_required"


def test_failed_payment_does_not_activate():
    settings.payment_enabled = True
    settings.tribute_enabled = True
    settings.tribute_api_key = "secret"
    repo, service = _service()
    service._providers["tribute"].create_payment = lambda **kwargs: PaymentProviderResult(provider="tribute", status="pending", external_payment_id="x-2", external_checkout_url="https://example.test/pay")
    out = service.create_payment_for_invoice(11, "tribute")
    intent_id = out["payment_intent_id"]
    repo.payment_intents[intent_id]["external_payment_id"] = "x-2"
    body = '{"name":"shop_order","payload":{"uuid":"x-2","status":"failed","amount":2900,"currency":"usd"}}'
    import hashlib, hmac
    sig = hmac.new(b"secret", body.encode("utf-8"), hashlib.sha256).hexdigest()
    result = service.handle_provider_webhook("tribute", {"trbt-signature": sig}, body)
    assert result["ok"] is False
    assert repo.invoices[11]["status"] != "paid"


def test_amount_mismatch_rejected():
    settings.payment_enabled = True
    settings.tribute_enabled = True
    settings.tribute_api_key = "secret"
    repo, service = _service()
    service._providers["tribute"].create_payment = lambda **kwargs: PaymentProviderResult(provider="tribute", status="pending", external_payment_id="x-3", external_checkout_url="https://example.test/pay")
    out = service.create_payment_for_invoice(11, "tribute")
    intent_id = out["payment_intent_id"]
    repo.payment_intents[intent_id]["external_payment_id"] = "x-3"
    body = '{"name":"shop_order","payload":{"uuid":"x-3","status":"paid","amount":100,"currency":"usd"}}'
    import hashlib, hmac
    sig = hmac.new(b"secret", body.encode("utf-8"), hashlib.sha256).hexdigest()
    result = service.handle_provider_webhook("tribute", {"trbt-signature": sig}, body)
    assert result["ok"] is False
    assert result["error"] == "amount_mismatch"


def test_payment_event_written_on_activation():
    settings.payment_enabled = True
    repo, service = _service()
    out = service.create_payment_for_invoice(11, "manual_bank_card")
    service.confirm_manual_payment(out["payment_intent_id"], admin_id=100, note="approved")
    assert any(e["event_type"] == "payment_received" for e in repo.billing_events)


def test_double_click_does_not_create_duplicate_active_lava_links():
    settings.payment_enabled = True
    settings.lava_top_enabled = True
    repo, service = _service()
    first = service.create_payment_for_invoice(11, "lava_top", attempt_id="a1", idempotency_key="a1")
    second = service.create_payment_for_invoice(11, "lava_top", attempt_id="a1", idempotency_key="a1")
    assert first["ok"] is True
    assert second.get("idempotent") is True
    assert first["payment_intent_id"] == second["payment_intent_id"]


def test_retry_after_failed_creates_new_attempt():
    settings.payment_enabled = True
    settings.lava_top_enabled = True
    repo, service = _service()
    first = service.create_payment_for_invoice(11, "lava_top", attempt_id="a2", idempotency_key="a2")
    repo.mark_payment_failed(first["payment_intent_id"], "provider_error")
    second = service.create_payment_for_invoice(11, "lava_top", attempt_id="a3", idempotency_key="a3")
    assert second["ok"] is True
    assert second["payment_intent_id"] != first["payment_intent_id"]


def test_tribute_client_create_shop_order_is_sync():
    assert not inspect.iscoroutinefunction(TributeClient.create_shop_order)


def test_tribute_provider_create_payment_inside_event_loop(monkeypatch):
    settings.payment_enabled = True
    settings.tribute_enabled = True
    settings.tribute_api_key = "secret"

    def _fake_create_shop_order(self, payload):
        return {"uuid": "order_123", "paymentUrl": "https://tribute.tg/pay/123", "webappPaymentUrl": "https://tribute.tg/webapp/pay/123"}

    monkeypatch.setattr(TributeClient, "create_shop_order", _fake_create_shop_order)
    provider = TributeProvider()
    async def _run_inside_loop():
        return provider.create_payment(invoice={"id": 35, "tenant_id": 5, "total": 9.0, "currency": "USD", "tariff_code": "basic"}, tenant={"id": 5})

    result = asyncio.run(_run_inside_loop())

    assert result.provider == "tribute"
    assert result.status == "pending"
    assert result.external_payment_id == "order_123"
    assert result.external_checkout_url == "https://tribute.tg/webapp/pay/123"


def test_tribute_provider_returns_failed_on_tribute_api_error(monkeypatch):
    settings.payment_enabled = True
    settings.tribute_enabled = True
    settings.tribute_api_key = "super-secret-key"

    def _raise_error(self, payload):
        raise TributeAPIError("tribute_api_request_error")

    monkeypatch.setattr(TributeClient, "create_shop_order", _raise_error)
    provider = TributeProvider()
    result = provider.create_payment(invoice={"id": 35, "tenant_id": 5, "total": 9.0, "currency": "USD", "tariff_code": "basic"}, tenant={"id": 5})

    assert result.status == "failed"
    assert result.error_text == "tribute_create_order_failed"
    assert "super-secret-key" not in str(result.error_text or "")


def test_tribute_provider_prefers_webapp_checkout_url(monkeypatch):
    settings.payment_enabled = True
    settings.tribute_enabled = True
    settings.tribute_api_key = "secret"

    def _fake_create_shop_order(self, payload):
        return {"uuid": "order_123", "paymentUrl": "https://tribute.tg/pay/123", "webappPaymentUrl": "https://tribute.tg/webapp/pay/123"}

    monkeypatch.setattr(TributeClient, "create_shop_order", _fake_create_shop_order)
    provider = TributeProvider()
    async def _run_inside_loop():
        return provider.create_payment(invoice={"id": 35, "tenant_id": 5, "total": 9.0, "currency": "USD", "tariff_code": "basic"}, tenant={"id": 5})

    result = asyncio.run(_run_inside_loop())

    assert result.provider == "tribute"
    assert result.status == "pending"
    assert result.external_payment_id == "order_123"
    assert result.external_checkout_url == "https://tribute.tg/webapp/pay/123"
