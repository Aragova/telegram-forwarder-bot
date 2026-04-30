from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from typing import Any

from aiogram.types import LabeledPrice

from app.config import settings
from app.payments.fixed_prices import get_stars_price

LOGGER = logging.getLogger("forwarder.payments.telegram_stars")


@dataclass(frozen=True, slots=True)
class StarsPayloadRef:
    payment_intent_id: int
    invoice_id: int
    user_id: int
    nonce: str
    raw: str


@dataclass(frozen=True, slots=True)
class StarsInvoiceContext:
    tenant_id: int
    user_id: int
    invoice_id: int
    payment_intent_id: int
    tariff_code: str
    period_months: int
    stars_amount: int
    currency: str
    title: str
    description: str
    payload: str


def build_stars_payload(*, payment_intent_id: int, invoice_id: int, user_id: int, nonce: str | None = None) -> str:
    safe_nonce = str(nonce or secrets.token_hex(4)).strip()
    return f"vimi:stars:pi:{int(payment_intent_id)}:inv:{int(invoice_id)}:user:{int(user_id)}:{safe_nonce}"


def parse_stars_payload(value: str) -> StarsPayloadRef:
    raw = str(value or "").strip()
    parts = raw.split(":")
    if len(parts) != 9 or parts[0] != "vimi" or parts[1] != "stars":
        raise ValueError("Некорректный payload оплаты")
    if parts[2] != "pi" or parts[4] != "inv" or parts[6] != "user":
        raise ValueError("Некорректные маркеры payload")
    payment_intent_id = int(parts[3] or 0)
    invoice_id = int(parts[5] or 0)
    user_id = int(parts[7] or 0)
    nonce = str(parts[8] or "").strip()
    if payment_intent_id <= 0 or invoice_id <= 0 or user_id <= 0 or not nonce:
        raise ValueError("Некорректные данные payload")
    return StarsPayloadRef(payment_intent_id=payment_intent_id, invoice_id=invoice_id, user_id=user_id, nonce=nonce, raw=raw)


class TelegramStarsService:
    def __init__(self, *, repo: Any, subscription_service: Any, billing_service: Any, invoice_service: Any, payment_service: Any, ensure_user_tenant: Any) -> None:
        self._repo = repo
        self._subscription_service = subscription_service
        self._billing_service = billing_service
        self._invoice_service = invoice_service
        self._payment_service = payment_service
        self._ensure_user_tenant = ensure_user_tenant

    async def create_stars_invoice_context(self, *, user_id: int, username: str | None, tariff_code: str, period_months: int, currency: str, method_code: str, attempt_id: str | None = None, idempotency_key: str | None = None) -> StarsInvoiceContext:
        _ = username
        if not settings.payment_enabled or not settings.telegram_stars_enabled:
            raise ValueError("Telegram Stars отключен")
        tenant_id = int(await self._ensure_user_tenant(int(user_id)))
        sub = await self._subscription_service.get_active_subscription(tenant_id)
        if sub:
            sub = await self._billing_service.ensure_billing_period(sub)
        else:
            from app.payments.payment_router import PaymentRouter
            sub = PaymentRouter._build_purchase_context(PaymentRouter, int(period_months))
        stars_amount = get_stars_price(tariff_code, int(period_months), repo=self._repo)
        if stars_amount is None:
            LOGGER.warning("STARS_PRICE_MISSING tenant_id=%s user_id=%s tariff_code=%s period_months=%s", tenant_id, user_id, tariff_code, period_months)
            raise ValueError("Цена в Telegram Stars не настроена")
        invoice_id = await self._invoice_service.create_draft_invoice(tenant_id, int(sub.get("id") or 0), str(sub.get("current_period_start")), str(sub.get("current_period_end")), currency="XTR")
        await self._invoice_service.add_invoice_item(int(invoice_id), item_type="base_plan", description=f"{tariff_code}:{int(period_months)}", quantity=1, unit_price=float(stars_amount), metadata={"plan_name": str(tariff_code).upper(), "period_months": int(period_months), "method_code": str(method_code), "provider": "telegram_stars", "currency": "XTR", "stars_amount": int(stars_amount)})
        await self._invoice_service.finalize_invoice(int(invoice_id))
        payment_intent_id = self._repo.create_payment_intent(tenant_id=tenant_id, invoice_id=int(invoice_id), provider="telegram_stars", status="created", amount=float(stars_amount), currency="XTR")
        payload = build_stars_payload(payment_intent_id=int(payment_intent_id), invoice_id=int(invoice_id), user_id=int(user_id))
        provider_payload_json = {"provider": "telegram_stars", "invoice_id": int(invoice_id), "payment_intent_id": int(payment_intent_id), "telegram_user_id": int(user_id), "tariff_code": str(tariff_code).lower(), "period_months": int(period_months), "stars_amount": int(stars_amount), "currency": "XTR", "payload": payload, "title": f"ViMi {str(tariff_code).upper()} — {int(period_months)} месяц", "description": "Подписка ViMi на выбранный период. Доступ активируется автоматически после оплаты.", "attempt_id": attempt_id, "idempotency_key": idempotency_key}
        self._repo.attach_provider_payload(int(payment_intent_id), provider_payload_json)
        return StarsInvoiceContext(tenant_id=tenant_id, user_id=int(user_id), invoice_id=int(invoice_id), payment_intent_id=int(payment_intent_id), tariff_code=str(tariff_code).lower(), period_months=int(period_months), stars_amount=int(stars_amount), currency="XTR", title=provider_payload_json["title"], description=provider_payload_json["description"], payload=payload)

    async def send_stars_invoice(self, *, bot: Any, chat_id: int, context: StarsInvoiceContext) -> None:
        LOGGER.info("STARS_INVOICE_SEND_START user_id=%s payment_intent_id=%s", context.user_id, context.payment_intent_id)
        await bot.send_invoice(chat_id=int(chat_id), title=context.title, description=context.description, payload=context.payload, provider_token="", currency="XTR", prices=[LabeledPrice(label=f"{context.tariff_code.upper()} {context.period_months} мес", amount=int(context.stars_amount))])
        self._repo.update_payment_intent_status(int(context.payment_intent_id), "pending")
        LOGGER.info("STARS_INVOICE_SENT user_id=%s payment_intent_id=%s", context.user_id, context.payment_intent_id)
