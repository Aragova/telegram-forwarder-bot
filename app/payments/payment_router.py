from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.billing_catalog import format_price
from app.payments import PaymentService as LavaPaymentService
from app.payments.payment_matrix import method_by_code


@dataclass(slots=True)
class PaymentStartResult:
    invoice_id: int
    provider: str
    method_title: str
    amount_text: str
    payment_url: str | None = None
    requires_receipt: bool = False


class PaymentRouter:
    def __init__(self, *, ensure_user_tenant: Any, subscription_service: Any, billing_service: Any, invoice_service: Any, payment_service: Any) -> None:
        self._ensure_user_tenant = ensure_user_tenant
        self._subscription_service = subscription_service
        self._billing_service = billing_service
        self._invoice_service = invoice_service
        self._payment_service = payment_service

    async def start_payment(
        self,
        *,
        user_id: int,
        tariff_code: str,
        period_months: int,
        currency: str,
        method_code: str,
        username: str | None = None,
    ) -> PaymentStartResult:
        method = method_by_code(currency, method_code) or {}
        provider = str(method.get("provider") or "")
        amount_text = format_price(tariff_code, int(period_months), currency)
        amount = float(amount_text.split()[0])

        tenant_id = await self._ensure_user_tenant(user_id)
        sub = await self._subscription_service.get_active_subscription(tenant_id)
        if not sub:
            raise ValueError("subscription_not_found")
        sub = await self._billing_service.ensure_billing_period(sub)
        invoice_id = await self._invoice_service.create_draft_invoice(
            int(tenant_id),
            int(sub.get("id") or 0),
            str(sub.get("current_period_start")),
            str(sub.get("current_period_end")),
            currency=currency,
        )
        await self._invoice_service.add_invoice_item(
            int(invoice_id),
            item_type="base_plan",
            description=f"{tariff_code}:{period_months}",
            quantity=1,
            unit_price=amount,
            metadata={"plan_name": str(tariff_code).upper(), "period_months": int(period_months), "method_code": method_code, "provider": provider},
        )
        await self._invoice_service.finalize_invoice(int(invoice_id))

        if provider == "lava_top":
            lava_service = LavaPaymentService()
            invoice_view = await lava_service.create_lava_invoice_for_user_invoice(
                user_id=user_id,
                invoice_id=int(invoice_id),
                tariff_code=tariff_code,
                amount=amount,
                currency=currency,
                username=username,
                payment_provider=method.get("payment_provider"),
            )
            return PaymentStartResult(int(invoice_id), provider, str(method.get("title") or "—"), amount_text, payment_url=invoice_view.payment_url)

        await self._payment_service.create_payment_for_invoice(int(invoice_id), provider)
        return PaymentStartResult(int(invoice_id), provider, str(method.get("title") or "—"), amount_text, requires_receipt=True)
