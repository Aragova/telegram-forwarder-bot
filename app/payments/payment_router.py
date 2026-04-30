from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import calendar
import inspect
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
    payment_intent_id: int | None = None
    attempt_id: str | None = None
    idempotent: bool = False
    status: str | None = None
    error_code: str | None = None
    error_text: str | None = None


class PaymentRouter:
    def __init__(self, *, ensure_user_tenant: Any, subscription_service: Any, billing_service: Any, invoice_service: Any, payment_service: Any) -> None:
        self._ensure_user_tenant = ensure_user_tenant
        self._subscription_service = subscription_service
        self._billing_service = billing_service
        self._invoice_service = invoice_service
        self._payment_service = payment_service
        self._pricing_repo = getattr(invoice_service, "_repo", None)

    async def _call_service(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    async def start_payment(
        self,
        *,
        user_id: int,
        tariff_code: str,
        period_months: int,
        currency: str,
        method_code: str,
        username: str | None = None,
        attempt_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> PaymentStartResult:
        method = method_by_code(currency, method_code) or {}
        provider = str(method.get("provider") or "")
        amount_text = format_price(tariff_code, int(period_months), currency, repo=self._pricing_repo)
        amount = float(amount_text.split()[0])

        tenant_id = await self._call_service(self._ensure_user_tenant, user_id)
        sub = await self._call_service(self._subscription_service.get_active_subscription, tenant_id)
        if sub:
            sub = await self._call_service(self._billing_service.ensure_billing_period, sub)
        else:
            sub = self._build_purchase_context(period_months)
        invoice_id = await self._call_service(
            self._invoice_service.create_draft_invoice,
            int(tenant_id),
            int(sub.get("id") or 0),
            str(sub.get("current_period_start")),
            str(sub.get("current_period_end")),
            currency=currency,
        )
        await self._call_service(
            self._invoice_service.add_invoice_item,
            int(invoice_id),
            item_type="base_plan",
            description=f"{tariff_code}:{period_months}",
            quantity=1,
            unit_price=amount,
            metadata={"plan_name": str(tariff_code).upper(), "period_months": int(period_months), "method_code": method_code, "provider": provider},
        )
        await self._call_service(self._invoice_service.finalize_invoice, int(invoice_id))

        if provider == "lava_top":
            intent_id: int | None = None
            repo = getattr(self._payment_service, "_repo", None)
            if repo and hasattr(repo, "create_payment_intent"):
                intent_id = repo.create_payment_intent(
                    tenant_id=int(tenant_id),
                    invoice_id=int(invoice_id),
                    provider="lava_top",
                    status="created",
                    amount=float(amount),
                    currency=str(currency or "USD").upper(),
                )
            if not intent_id:
                return PaymentStartResult(
                    int(invoice_id),
                    provider,
                    str(method.get("title") or "—"),
                    amount_text,
                    payment_intent_id=None,
                    status="provider_failed",
                    error_code="create_payment_intent_failed",
                    error_text="Не удалось создать ссылку оплаты",
                )
            lava_service = LavaPaymentService()
            invoice_view = await self._call_service(
                lava_service.create_lava_invoice_for_user_invoice,
                user_id=user_id,
                invoice_id=int(invoice_id),
                tariff_code=tariff_code,
                amount=amount,
                currency=currency,
                username=username,
                payment_provider=method.get("payment_provider"),
            )
            if not invoice_view.payment_url:
                return PaymentStartResult(
                    int(invoice_id),
                    provider,
                    str(method.get("title") or "—"),
                    amount_text,
                    payment_intent_id=int(intent_id),
                    status="pending",
                )
            if repo and hasattr(repo, "attach_provider_payload"):
                payload = {
                    "lava_invoice_id": str(invoice_view.invoice_id or ""),
                    "lava_amount_total": {"amount": float(invoice_view.amount), "currency": str(invoice_view.currency).upper()},
                    "lava_status": "new",
                    "lava_payment_url": str(invoice_view.payment_url),
                    "status": "new",
                    "id": str(invoice_view.invoice_id or ""),
                    "paymentUrl": str(invoice_view.payment_url),
                    "amountTotal": {"amount": float(invoice_view.amount), "currency": str(invoice_view.currency).upper()},
                    "tariff_code": str(tariff_code),
                    "period_months": int(period_months),
                    "currency": str(currency or "").upper(),
                    "amount_text": str(amount_text),
                    "method_code": str(method_code),
                    "payment_provider": method.get("payment_provider"),
                    "payment_method": method.get("payment_method"),
                    "offer_id": str(tariff_code),
                }
                repo.attach_provider_payload(int(intent_id), payload)
            if repo and hasattr(repo, "attach_checkout_url"):
                repo.attach_checkout_url(int(intent_id), str(invoice_view.payment_url), external_payment_id=str(invoice_view.invoice_id or ""))
            if repo and hasattr(repo, "update_payment_intent_status"):
                repo.update_payment_intent_status(int(intent_id), "checkout_opened")
            return PaymentStartResult(
                int(invoice_id),
                provider,
                str(method.get("title") or "—"),
                amount_text,
                payment_url=invoice_view.payment_url,
                payment_intent_id=int(intent_id),
                status="checkout_opened",
            )

        payment_result = await self._call_service(
            self._payment_service.create_payment_for_invoice,
            int(invoice_id),
            provider,
            attempt_id=attempt_id,
            idempotency_key=idempotency_key,
        )
        return PaymentStartResult(
            int(invoice_id),
            provider,
            str(method.get("title") or "—"),
            amount_text,
            payment_url=payment_result.get("checkout_url") if isinstance(payment_result, dict) else None,
            requires_receipt=provider in {"manual_bank_card", "card_provider", "sbp_provider", "crypto_manual"},
            payment_intent_id=(int(payment_result.get("payment_intent_id") or 0) or None) if isinstance(payment_result, dict) else None,
            attempt_id=attempt_id,
            idempotent=bool(payment_result.get("idempotent")) if isinstance(payment_result, dict) else False,
            status=str(payment_result.get("status") or "") if isinstance(payment_result, dict) else None,
            error_code=str(payment_result.get("error") or "") if isinstance(payment_result, dict) else None,
            error_text=str(payment_result.get("error_text") or payment_result.get("message_ru") or "") if isinstance(payment_result, dict) else None,
        )

    def _build_purchase_context(self, period_months: int) -> dict[str, Any]:
        started = datetime.now(timezone.utc)
        ended = self._add_months(started, int(period_months))
        return {
            "id": 0,
            "current_period_start": started.date().isoformat(),
            "current_period_end": ended.date().isoformat(),
        }

    @staticmethod
    def _add_months(value: datetime, months: int) -> datetime:
        safe_months = max(int(months), 1)
        month = value.month - 1 + safe_months
        year = value.year + month // 12
        month = month % 12 + 1
        day = min(value.day, calendar.monthrange(year, month)[1])
        return value.replace(year=year, month=month, day=day)
