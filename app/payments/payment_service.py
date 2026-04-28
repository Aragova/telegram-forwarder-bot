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
        _ = username
        if not settings.lava_top_enabled:
            raise RuntimeError("Оплата через Lava.top сейчас выключена")

        tariff = get_tariff("basic")
        user_email = (email or "").strip() or f"user_{int(user_id)}@usevimi.local"
        client_order_id = f"vimi:{int(user_id)}:{tariff.code}:{int(time.time())}:{uuid.uuid4().hex[:8]}"

        result: LavaTopInvoiceResult = await self._lava_client.create_invoice(
            email=user_email,
            offer_id=tariff.lava_offer_id,
            currency=tariff.currency,
            buyer_language="RU",
            client_order_id=client_order_id,
        )

        return PaymentInvoiceView(
            invoice_id=result.invoice_id,
            payment_url=result.payment_url,
            amount=result.amount,
            currency=result.currency,
            tariff_code=tariff.code,
            provider="lava_top",
        )
