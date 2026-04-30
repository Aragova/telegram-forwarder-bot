import asyncio

from app.config import settings
from app.payments.telegram_stars_service import TelegramStarsService


class RepoStub:
    def create_payment_intent(self, **kwargs):
        return 555

    def attach_provider_payload(self, payment_intent_id, payload):
        return None

    def update_payment_intent_status(self, payment_intent_id, status):
        return None

    def get_billing_fixed_prices(self, kind):
        return {"basic": {"1": {"stars": 1}}}


class SubscriptionServiceStub:
    def get_active_subscription(self, tenant_id):
        return None


class BillingServiceStub:
    def ensure_billing_period(self, sub):
        return sub


class InvoiceServiceStub:
    def create_draft_invoice(self, tenant_id, sub_id, period_start, period_end, currency="XTR"):
        return 321

    def add_invoice_item(self, invoice_id, **kwargs):
        return None

    def finalize_invoice(self, invoice_id):
        return None


def test_create_context_supports_sync_ensure_user_tenant():
    service = TelegramStarsService(
        repo=RepoStub(),
        subscription_service=SubscriptionServiceStub(),
        billing_service=BillingServiceStub(),
        invoice_service=InvoiceServiceStub(),
        payment_service=object(),
        ensure_user_tenant=lambda user_id: 77,
    )


    prev_payment_enabled = settings.payment_enabled
    prev_stars_enabled = settings.telegram_stars_enabled
    settings.payment_enabled = True
    settings.telegram_stars_enabled = True
    try:
        ctx = asyncio.run(service.create_stars_invoice_context(
        user_id=123,
        username="u",
        tariff_code="basic",
        period_months=1,
        currency="XTR",
        method_code="telegram_stars",
        ))
    finally:
        settings.payment_enabled = prev_payment_enabled
        settings.telegram_stars_enabled = prev_stars_enabled

    assert ctx.tenant_id == 77
    assert ctx.invoice_id == 321
    assert ctx.payment_intent_id == 555


def test_call_maybe_async_supports_sync_and_async_results():
    service = TelegramStarsService(
        repo=RepoStub(),
        subscription_service=SubscriptionServiceStub(),
        billing_service=BillingServiceStub(),
        invoice_service=InvoiceServiceStub(),
        payment_service=object(),
        ensure_user_tenant=lambda user_id: 1,
    )

    sync_result = asyncio.run(service._call_maybe_async(lambda value: value + 1, 10))

    async def async_add(value):
        return value + 2

    async_result = asyncio.run(service._call_maybe_async(async_add, 10))

    assert sync_result == 11
    assert async_result == 12
