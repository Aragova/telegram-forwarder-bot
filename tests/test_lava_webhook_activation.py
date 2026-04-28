from __future__ import annotations

import base64
import hashlib
import hmac

from app.config import settings
from app.payments.lava_webhook_activation import LavaWebhookActivationService, verify_lava_webhook_auth


class _FakeRepo:
    def __init__(self) -> None:
        self.invoices = {
            15: {"id": 15, "tenant_id": 77, "status": "open", "currency": "USD", "total": 9.0},
            16: {"id": 16, "tenant_id": 77, "status": "paid", "currency": "USD", "total": 9.0},
        }
        self.subscriptions = {77: {"id": 1, "tenant_id": 77, "plan_name": "FREE", "plan_id": 1, "status": "active", "priority_level": 1, "price": 0.0}}
        self.events: list[dict] = []

    def get_invoice(self, invoice_id: int):
        return self.invoices.get(invoice_id)

    def set_invoice_status(self, invoice_id: int, status: str, **kwargs):
        self.invoices[invoice_id]["status"] = status
        self.invoices[invoice_id]["external_reference"] = kwargs.get("external_reference")
        return True

    def create_billing_event(self, tenant_id: int, event_type: str, **kwargs):
        self.events.append({"tenant_id": tenant_id, "event_type": event_type, **kwargs})
        return len(self.events)

    def get_active_subscription(self, tenant_id: int):
        return self.subscriptions.get(tenant_id)

    def get_plan_by_name(self, plan_name: str):
        return {"id": 2, "name": str(plan_name).upper(), "priority_level": 2, "price": 9.0}

    def replace_subscription_plan(self, subscription_id: int, plan_id: int):
        self.subscriptions[77]["plan_name"] = "BASIC"
        self.subscriptions[77]["plan_id"] = plan_id
        return True

    def add_subscription_history(self, **kwargs):
        return 1


def _payload(status: str = "paid", order: str = "vimi:invoice:15:user:77:tariff:basic:uuid"):
    return {"id": "lava-1", "status": status, "clientOrderId": order}


def test_webhook_paid_status_activates_basic():
    repo = _FakeRepo()
    sent = []
    service = LavaWebhookActivationService(repo, notifier=lambda uid, text: sent.append((uid, text)))
    out = service.process_webhook(_payload("paid"), '{"id":"lava-1"}')
    assert out.code == "paid_activated"
    assert repo.invoices[15]["status"] == "paid"
    assert repo.subscriptions[77]["plan_name"] == "BASIC"
    assert sent and sent[0][0] == 77


def test_webhook_pending_status_does_not_activate():
    repo = _FakeRepo()
    service = LavaWebhookActivationService(repo)
    out = service.process_webhook(_payload("pending"), '{}')
    assert out.code == "ignored_pending"
    assert repo.invoices[15]["status"] == "open"


def test_webhook_failed_status_does_not_activate():
    repo = _FakeRepo()
    service = LavaWebhookActivationService(repo)
    out = service.process_webhook(_payload("failed"), '{}')
    assert out.code == "ignored_failed"
    assert repo.invoices[15]["status"] == "open"


def test_invalid_client_order_id_is_unmatched():
    repo = _FakeRepo()
    service = LavaWebhookActivationService(repo)
    out = service.process_webhook(_payload("paid", order="broken"), '{}')
    assert out.code == "unmatched"
    assert repo.invoices[15]["status"] == "open"


def test_foreign_user_invoice_pair_is_unmatched():
    repo = _FakeRepo()
    service = LavaWebhookActivationService(repo)
    out = service.process_webhook(_payload("paid", order="vimi:invoice:15:user:999:tariff:basic:uuid"), '{}')
    assert out.code == "unmatched"
    assert repo.invoices[15]["status"] == "open"


def test_duplicate_paid_webhook_not_extended_twice():
    repo = _FakeRepo()
    service = LavaWebhookActivationService(repo)
    first = service.process_webhook(_payload("paid"), '{}')
    second = service.process_webhook(_payload("paid"), '{}')
    assert first.code == "paid_activated"
    assert second.code == "duplicate"


def test_already_paid_invoice_not_extended_twice():
    repo = _FakeRepo()
    service = LavaWebhookActivationService(repo)
    out = service.process_webhook(_payload("paid", order="vimi:invoice:16:user:77:tariff:basic:uuid"), '{}')
    assert out.code == "duplicate"


def test_webhook_auth_without_basic_rejected():
    old_login = settings.lava_top_webhook_login
    old_password = settings.lava_top_webhook_password
    settings.lava_top_webhook_login = "lava"
    settings.lava_top_webhook_password = "pass"
    try:
        result = verify_lava_webhook_auth({}, "{}")
        assert result.ok is False
    finally:
        settings.lava_top_webhook_login = old_login
        settings.lava_top_webhook_password = old_password


def test_webhook_auth_with_valid_basic_passes():
    old_login = settings.lava_top_webhook_login
    old_password = settings.lava_top_webhook_password
    settings.lava_top_webhook_login = "lava"
    settings.lava_top_webhook_password = "pass"
    token = base64.b64encode(b"lava:pass").decode("utf-8")
    try:
        result = verify_lava_webhook_auth({"Authorization": f"Basic {token}"}, "{}")
        assert result.ok is True
    finally:
        settings.lava_top_webhook_login = old_login
        settings.lava_top_webhook_password = old_password


def test_webhook_signature_secret_supported():
    old_secret = settings.lava_top_webhook_secret
    settings.lava_top_webhook_secret = "secret"
    body = '{"id":"lava-1"}'
    sig = hmac.new(b"secret", body.encode("utf-8"), hashlib.sha256).hexdigest()
    try:
        result = verify_lava_webhook_auth({"X-LavaTop-Signature": sig}, body)
        assert result.ok is True
    finally:
        settings.lava_top_webhook_secret = old_secret
