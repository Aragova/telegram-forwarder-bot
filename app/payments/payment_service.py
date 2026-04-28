from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from app.config import settings
from app.payments.lava_top_client import LavaTopClient, LavaTopInvoiceResult
from app.tariffs import get_tariff


@dataclass(frozen=True, slots=True)
class PaymentInvoiceView:
    invoice_id: str
    payment_url: str
    amount: float
    currency: str
    tariff_code: str
    provider: str


class PaymentService:
    def __init__(self, *, lava_client: LavaTopClient | None = None) -> None:
        self._lava_client = lava_client or LavaTopClient()

    async def create_lava_basic_invoice(
        self,
        *,
        user_id: int,
        username: str | None,
        email: str | None = None,
    ) -> PaymentInvoiceView:
        basic_tariff = get_tariff("basic")
        return await self.create_lava_invoice_for_user_invoice(
            user_id=user_id,
            invoice_id=0,
            tariff_code="basic",
            amount=basic_tariff.price,
            currency=basic_tariff.currency,
            username=username,
            email=email,
        )

    async def create_lava_invoice_for_user_invoice(
        self,
        *,
        user_id: int,
        invoice_id: int,
        tariff_code: str,
        amount: float,
        currency: str,
        username: str | None = None,
        email: str | None = None,
    ) -> PaymentInvoiceView:
        _ = username
        if not settings.lava_top_enabled:
            raise RuntimeError("Оплата через Lava.top сейчас выключена")

        tariff = get_tariff(tariff_code)
        user_email = (email or "").strip() or f"user_{int(user_id)}@usevimi.local"
        client_order_id = (
            f"vimi:invoice:{int(invoice_id)}:user:{int(user_id)}:{tariff.code}:{uuid.uuid4().hex[:8]}"
            if int(invoice_id) > 0
            else f"vimi:{int(user_id)}:{tariff.code}:{int(time.time())}:{uuid.uuid4().hex[:8]}"
        )

        result: LavaTopInvoiceResult = await self._lava_client.create_invoice(
            email=user_email,
            offer_id=tariff.lava_offer_id,
            currency=str(currency or tariff.currency).upper(),
            buyer_language="RU",
            client_order_id=client_order_id,
        )

        return PaymentInvoiceView(
            invoice_id=result.invoice_id,
            payment_url=result.payment_url,
            amount=float(amount),
            currency=str(currency or result.currency).upper(),
            tariff_code=tariff.code,
            provider="lava_top",
        )
