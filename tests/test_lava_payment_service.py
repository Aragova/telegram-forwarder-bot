from __future__ import annotations

import asyncio

from app.config import settings
from app.payments.lava_top_client import LavaTopInvoiceResult
from app.payments.payment_service import PaymentService


class _FakeLavaClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def create_invoice(self, **kwargs):
        self.calls.append(kwargs)
        return LavaTopInvoiceResult(
            invoice_id="inv_1",
            status="new",
            amount=9.0,
            currency="USD",
            payment_url="https://gate.lava.top/pay/abc",
            raw={"status": "new"},
        )


def test_lava_disabled_returns_clear_error():
    old = settings.lava_top_enabled
    settings.lava_top_enabled = False
    service = PaymentService(lava_client=_FakeLavaClient())
    try:
        asyncio.run(service.create_lava_basic_invoice(user_id=42, username="u"))
    except RuntimeError as exc:
        assert "выключена" in str(exc)
    else:
        assert False, "Expected RuntimeError"
    finally:
        settings.lava_top_enabled = old


def test_basic_offer_id_and_technical_email_and_client_order_id_shape():
    old_enabled = settings.lava_top_enabled
    old_offer = settings.lava_top_basic_offer_id
    settings.lava_top_enabled = True
    settings.lava_top_basic_offer_id = "offer-basic-123"

    fake_client = _FakeLavaClient()
    service = PaymentService(lava_client=fake_client)
    view = asyncio.run(service.create_lava_basic_invoice(user_id=77, username="demo", email=None))

    assert fake_client.calls
    payload = fake_client.calls[0]
    assert payload["offer_id"] == "offer-basic-123"
    assert payload["email"] == "user_77@usevimi.local"
    assert "vimi:77:basic:" in payload["client_order_id"]
    assert view.provider == "lava_top"
    assert view.tariff_code == "basic"

    settings.lava_top_enabled = old_enabled
    settings.lava_top_basic_offer_id = old_offer


def test_client_order_id_is_unique_and_status_new_does_not_activate_anything():
    old_enabled = settings.lava_top_enabled
    settings.lava_top_enabled = True

    fake_client = _FakeLavaClient()
    service = PaymentService(lava_client=fake_client)
    asyncio.run(service.create_lava_basic_invoice(user_id=101, username="u"))
    asyncio.run(service.create_lava_basic_invoice(user_id=101, username="u"))

    first = fake_client.calls[0]["client_order_id"]
    second = fake_client.calls[1]["client_order_id"]
    assert first != second
    assert all(call.get("buyer_language") == "RU" for call in fake_client.calls)

    settings.lava_top_enabled = old_enabled
