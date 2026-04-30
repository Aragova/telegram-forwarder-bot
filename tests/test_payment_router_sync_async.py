import asyncio

from app.payments.payment_router import PaymentRouter


class _SubscriptionServiceSync:
    def get_active_subscription(self, tenant_id: int):
        return {
            "id": 11,
            "current_period_start": "2026-01-01",
            "current_period_end": "2026-02-01",
        }


class _SubscriptionServiceAsync:
    async def get_active_subscription(self, tenant_id: int):
        return {
            "id": 12,
            "current_period_start": "2026-01-01",
            "current_period_end": "2026-02-01",
        }


class _SubscriptionServiceEmpty:
    def get_active_subscription(self, tenant_id: int):
        return None


class _BillingService:
    def ensure_billing_period(self, sub: dict):
        return sub


class _InvoiceService:
    def __init__(self):
        self.created = []
        self.items = []
        self.finalized = []

    def create_draft_invoice(self, tenant_id, subscription_id, period_start, period_end, currency="RUB"):
        self.created.append((tenant_id, subscription_id, period_start, period_end, currency))
        return 101

    def add_invoice_item(self, invoice_id, **kwargs):
        self.items.append((invoice_id, kwargs))

    def finalize_invoice(self, invoice_id):
        self.finalized.append(invoice_id)


class _PricingRepo:
    def get_billing_usd_prices(self):
        return {"basic": {1: 10.0}}

    def get_billing_exchange_rates(self):
        return {"USD_TO_UAH": 50.0}


class _PaymentService:
    def __init__(self):
        self.calls = []
        self._repo = _PaymentRepo()

    def create_payment_for_invoice(self, invoice_id: int, provider: str, **kwargs):
        self.calls.append((invoice_id, provider, kwargs))

class _PaymentRepo:
    def __init__(self):
        self.intents = {}
        self.seq = 0

    def create_payment_intent(self, **payload):
        self.seq += 1
        self.intents[self.seq] = {"id": self.seq, **payload}
        return self.seq

    def attach_provider_payload(self, payment_intent_id: int, payload: dict):
        self.intents[payment_intent_id]["provider_payload_json"] = payload
        return True

    def attach_checkout_url(self, payment_intent_id: int, checkout_url: str, *, external_payment_id=None):
        self.intents[payment_intent_id]["external_checkout_url"] = checkout_url
        self.intents[payment_intent_id]["external_payment_id"] = external_payment_id
        return True

    def update_payment_intent_status(self, payment_intent_id: int, status: str, *, error_text=None):
        self.intents[payment_intent_id]["status"] = status
        return True


def _build_router(subscription_service):
    invoice_service = _InvoiceService()
    payment_service = _PaymentService()
    router = PaymentRouter(
        ensure_user_tenant=lambda user_id: 7,
        subscription_service=subscription_service,
        billing_service=_BillingService(),
        invoice_service=invoice_service,
        payment_service=payment_service,
    )
    return router, invoice_service, payment_service


def _build_router_with_pricing_repo(subscription_service):
    invoice_service = _InvoiceService()
    invoice_service._repo = _PricingRepo()
    payment_service = _PaymentService()
    router = PaymentRouter(
        ensure_user_tenant=lambda user_id: 7,
        subscription_service=subscription_service,
        billing_service=_BillingService(),
        invoice_service=invoice_service,
        payment_service=payment_service,
    )
    return router, invoice_service, payment_service


def test_start_payment_supports_sync_subscription_service():
    router, invoice_service, payment_service = _build_router(_SubscriptionServiceSync())

    result = asyncio.run(
        router.start_payment(
            user_id=55,
            tariff_code="basic",
            period_months=1,
            currency="RUB",
            method_code="rub_card_sbp",
            username="demo",
        )
    )

    assert invoice_service.created
    assert payment_service.calls == [(101, "manual_bank_card", {"attempt_id": None, "idempotency_key": None})]
    assert result.requires_receipt is True


def test_start_payment_supports_async_subscription_service():
    router, _, payment_service = _build_router(_SubscriptionServiceAsync())

    result = asyncio.run(
        router.start_payment(
            user_id=77,
            tariff_code="basic",
            period_months=1,
            currency="RUB",
            method_code="rub_card_sbp",
        )
    )

    assert payment_service.calls == [(101, "manual_bank_card", {"attempt_id": None, "idempotency_key": None})]
    assert result.requires_receipt is True


def test_start_payment_manual_allows_first_purchase_without_active_subscription():
    router, invoice_service, payment_service = _build_router(_SubscriptionServiceEmpty())

    result = asyncio.run(
        router.start_payment(
            user_id=99,
            tariff_code="basic",
            period_months=1,
            currency="RUB",
            method_code="rub_card_sbp",
        )
    )

    created = invoice_service.created[0]
    assert created[1] == 0
    assert created[2]
    assert created[3]
    assert payment_service.calls == [(101, "manual_bank_card", {"attempt_id": None, "idempotency_key": None})]
    assert result.requires_receipt is True


def test_start_payment_lava_allows_first_purchase_without_active_subscription(monkeypatch):
    class _FakeLavaView:
        invoice_id = "inv_101"
        payment_url = "https://lava.test/pay/101"
        amount = 10.0
        currency = "RUB"

    class _FakeLavaService:
        async def create_lava_invoice_for_user_invoice(self, **kwargs):
            return _FakeLavaView()

    from app.payments import payment_router as payment_router_module

    monkeypatch.setattr(payment_router_module, "LavaPaymentService", _FakeLavaService)
    router, _, payment_service = _build_router(_SubscriptionServiceEmpty())

    result = asyncio.run(
        router.start_payment(
            user_id=100,
            tariff_code="basic",
            period_months=1,
            currency="RUB",
            method_code="rub_lava",
        )
    )

    assert payment_service.calls == []
    assert result.payment_url == "https://lava.test/pay/101"
    assert result.payment_intent_id == 1
    assert result.status == "checkout_opened"
    intent = payment_service._repo.intents[1]
    assert intent["provider"] == "lava_top"
    assert intent["status"] == "checkout_opened"
    assert intent["provider_payload_json"]["lava_invoice_id"] == "inv_101"
    assert intent["provider_payload_json"]["lava_amount_total"]["currency"] == "RUB"


def test_start_payment_uses_repo_usd_and_uah_rate_for_new_payment():
    router, _, payment_service = _build_router_with_pricing_repo(_SubscriptionServiceEmpty())
    payment_service.create_payment_for_invoice = lambda invoice_id, provider, **kwargs: {"payment_intent_id": 5, "status": "waiting_confirmation"}
    result = asyncio.run(
        router.start_payment(
            user_id=101,
            tariff_code="basic",
            period_months=1,
            currency="UAH",
            method_code="uah_abank",
        )
    )
    assert result.amount_text == "500 UAH"


def test_start_payment_lava_without_payment_url_does_not_expose_link(monkeypatch):
    class _FakeLavaView:
        invoice_id = "inv_102"
        payment_url = ""
        amount = 10.0
        currency = "USD"

    class _FakeLavaService:
        async def create_lava_invoice_for_user_invoice(self, **kwargs):
            return _FakeLavaView()

    from app.payments import payment_router as payment_router_module

    monkeypatch.setattr(payment_router_module, "LavaPaymentService", _FakeLavaService)
    router, _, _payment_service = _build_router(_SubscriptionServiceEmpty())
    result = asyncio.run(
        router.start_payment(
            user_id=222,
            tariff_code="basic",
            period_months=1,
            currency="USD",
            method_code="lava_card_usd",
        )
    )
    assert result.payment_url is None
    assert result.status == "pending"
