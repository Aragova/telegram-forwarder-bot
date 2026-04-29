from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LavaClientOrderRef:
    internal_invoice_id: int
    user_id: int
    tariff_code: str
    raw: str


def parse_lava_client_order_id(value: str) -> LavaClientOrderRef:
    raw = str(value or "").strip()
    parts = raw.split(":")
    if len(parts) != 8:
        raise ValueError("Некорректный формат clientOrderId")
    if parts[0] != "vimi" or parts[1] != "invoice" or parts[3] != "user" or parts[5] != "tariff":
        raise ValueError("Некорректный формат clientOrderId")
    invoice_id = int(parts[2])
    user_id = int(parts[4])
    tariff_code = str(parts[6] or "").strip().lower()
    nonce = str(parts[7] or "").strip()
    if invoice_id < 0 or user_id <= 0 or not tariff_code or not nonce:
        raise ValueError("Некорректный формат clientOrderId")
    return LavaClientOrderRef(
        internal_invoice_id=invoice_id,
        user_id=user_id,
        tariff_code=tariff_code,
        raw=raw,
    )

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
        payment_provider: str | None = None,
        payment_method: str | None = None,
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
        payment_provider: str | None = None,
        payment_method: str | None = None,
    ) -> PaymentInvoiceView:
        _ = username
        if not settings.lava_top_enabled:
            raise RuntimeError("Оплата через Lava.top сейчас выключена")

        tariff = get_tariff(tariff_code)
        user_email = (email or "").strip() or f"user_{int(user_id)}@usevimi.local"
        client_order_id = (
            f"vimi:invoice:{int(invoice_id)}:user:{int(user_id)}:tariff:{tariff.code}:{uuid.uuid4().hex}"
        )

        result: LavaTopInvoiceResult = await self._lava_client.create_invoice(
            email=user_email,
            offer_id=tariff.lava_offer_id,
            currency=str(currency or tariff.currency).upper(),
            buyer_language="RU",
            client_order_id=client_order_id,
            payment_provider=payment_provider,
            payment_method=payment_method,
        )

        return PaymentInvoiceView(
            invoice_id=result.invoice_id,
            payment_url=result.payment_url,
            amount=float(amount),
            currency=str(currency or result.currency).upper(),
            tariff_code=tariff.code,
            provider="lava_top",
        )
