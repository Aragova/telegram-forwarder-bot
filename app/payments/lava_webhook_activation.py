from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from aiogram import Bot

from app.config import settings
from app.payments.lava_top_client import sanitize_lava_response_for_log
from app.payments.payment_service import LavaClientOrderRef, parse_lava_client_order_id
from app.subscription_service import SubscriptionService

LOGGER = logging.getLogger("forwarder.payments.lava.webhook")

PAID_STATUSES = {"paid", "success", "completed", "succeeded"}
PENDING_STATUSES = {"new", "open", "pending"}
FAILED_STATUSES = {"failed", "cancelled", "canceled", "expired"}


@dataclass(frozen=True, slots=True)
class LavaWebhookAuthResult:
    ok: bool
    reason: str = ""


@dataclass(frozen=True, slots=True)
class LavaWebhookProcessResult:
    ok: bool
    http_status: int
    code: str


def _safe_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": payload.get("id") or payload.get("invoiceId") or payload.get("invoice_id"),
        "clientOrderId": payload.get("clientOrderId") or payload.get("client_order_id"),
        "status": payload.get("status"),
    }


def _normalize_status(raw_status: str) -> str:
    status = str(raw_status or "").strip().lower()
    if status in PAID_STATUSES:
        return "paid"
    if status in PENDING_STATUSES:
        return "pending"
    if status in FAILED_STATUSES:
        return "failed"
    return "unknown"


def verify_lava_webhook_auth(headers: dict[str, str], raw_body: str) -> LavaWebhookAuthResult:
    auth_header = str(headers.get("Authorization") or "")
    expected_login = str(settings.lava_top_webhook_login or "").strip()
    expected_password = str(settings.lava_top_webhook_password or "")
    if expected_login or expected_password:
        if not auth_header.lower().startswith("basic "):
            return LavaWebhookAuthResult(ok=False, reason="basic_auth_required")
        try:
            token = auth_header.split(" ", 1)[1]
            decoded = base64.b64decode(token).decode("utf-8")
            login, password = decoded.split(":", 1)
        except Exception:
            return LavaWebhookAuthResult(ok=False, reason="basic_auth_invalid")
        if login != expected_login or password != expected_password:
            return LavaWebhookAuthResult(ok=False, reason="basic_auth_invalid")

    secret = str(settings.lava_top_webhook_secret or "").strip()
    if secret:
        provided = str(headers.get("X-LavaTop-Signature") or headers.get("X-Signature") or "")
        if not provided:
            return LavaWebhookAuthResult(ok=False, reason="signature_required")
        expected = hmac.new(secret.encode("utf-8"), raw_body.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(provided, expected):
            return LavaWebhookAuthResult(ok=False, reason="signature_invalid")

    return LavaWebhookAuthResult(ok=True)


class LavaWebhookActivationService:
    def __init__(self, repo, *, notifier: Callable[[int, str], None] | None = None) -> None:
        self._repo = repo
        self._subscription_service = SubscriptionService(repo)
        self._notifier = notifier

    def process_webhook(self, payload: dict[str, Any], raw_body: str) -> LavaWebhookProcessResult:
        keys = sorted(payload.keys())
        LOGGER.info(
            "LAVA_WEBHOOK_RECEIVED_KEYS | keys=%s | contractId_present=%s | parentContractId_present=%s",
            keys,
            payload.get("contractId") is not None,
            (payload.get("ParentContractId") is not None) or (payload.get("parentContractId") is not None),
        )
        LOGGER.debug("LAVA_WEBHOOK_RECEIVED_SAFE | payload=%s", sanitize_lava_response_for_log(payload))

        safe = _safe_payload(payload)
        provider_invoice_id = str(safe.get("id") or "")
        provider_event_id = str(payload.get("eventId") or payload.get("event_id") or provider_invoice_id or "")
        normalized_status = _normalize_status(str(safe.get("status") or ""))

        client_order_raw = str(safe.get("clientOrderId") or "")
        try:
            order_ref: LavaClientOrderRef = parse_lava_client_order_id(client_order_raw)
        except Exception:
            LOGGER.warning("Lava webhook unmatched: битый clientOrderId payload=%s", safe)
            self._record_event(tenant_id=0, event_type="lava_webhook_unmatched", metadata={"reason": "invalid_client_order_id", "payload": safe})
            return LavaWebhookProcessResult(ok=True, http_status=202, code="unmatched")

        invoice = self._repo.get_invoice(int(order_ref.internal_invoice_id)) if hasattr(self._repo, "get_invoice") else None
        if not invoice:
            LOGGER.warning("Lava webhook unmatched: invoice не найден invoice_id=%s", order_ref.internal_invoice_id)
            self._record_event(tenant_id=0, event_type="lava_webhook_unmatched", metadata={"reason": "invoice_not_found", "payload": safe})
            return LavaWebhookProcessResult(ok=True, http_status=202, code="unmatched")

        tenant_id = int(invoice.get("tenant_id") or 0)
        if tenant_id != int(order_ref.user_id):
            LOGGER.warning("Lava webhook unmatched: user/invoice mismatch invoice_id=%s tenant_id=%s parsed_user_id=%s", order_ref.internal_invoice_id, tenant_id, order_ref.user_id)
            self._record_event(tenant_id=tenant_id, event_type="lava_webhook_unmatched", metadata={"reason": "user_invoice_mismatch", "payload": safe})
            return LavaWebhookProcessResult(ok=True, http_status=202, code="unmatched")

        if normalized_status == "unknown":
            LOGGER.info("Lava webhook unknown status invoice_id=%s status=%s", order_ref.internal_invoice_id, safe.get("status"))
            self._record_event(tenant_id=tenant_id, event_type="lava_webhook_unknown_status", metadata={"status": safe.get("status"), "payload": safe})
            return LavaWebhookProcessResult(ok=True, http_status=200, code="ignored_unknown_status")

        if normalized_status != "paid":
            self._record_event(
                tenant_id=tenant_id,
                event_type="lava_webhook_received",
                metadata={"status": normalized_status, "provider_invoice_id": provider_invoice_id, "provider_event_id": provider_event_id, "invoice_id": int(order_ref.internal_invoice_id)},
            )
            return LavaWebhookProcessResult(ok=True, http_status=200, code=f"ignored_{normalized_status}")

        if str(invoice.get("status") or "").lower() == "paid":
            self._record_event(tenant_id=tenant_id, event_type="lava_webhook_duplicate", metadata={"invoice_id": int(order_ref.internal_invoice_id), "provider_event_id": provider_event_id})
            return LavaWebhookProcessResult(ok=True, http_status=200, code="duplicate")

        external_reference = provider_invoice_id or self._fallback_event_id(raw_body=raw_body, provider_invoice_id=provider_invoice_id, status="paid")
        if hasattr(self._repo, "set_invoice_status"):
            self._repo.set_invoice_status(
                int(order_ref.internal_invoice_id),
                "paid",
                paid_at=datetime.now(timezone.utc).isoformat(),
                external_reference=external_reference,
            )

        self._subscription_service.change_plan(
            int(order_ref.user_id),
            str(order_ref.tariff_code).upper(),
            changed_by="lava_webhook",
            reason="lava_paid",
            effective_mode="immediate",
        )
        self._record_event(
            tenant_id=tenant_id,
            event_type="lava_webhook_paid",
            metadata={
                "invoice_id": int(order_ref.internal_invoice_id),
                "provider_invoice_id": provider_invoice_id,
                "provider_event_id": provider_event_id,
                "tariff_code": order_ref.tariff_code,
            },
        )
        self._notify_user(int(order_ref.user_id))
        return LavaWebhookProcessResult(ok=True, http_status=200, code="paid_activated")

    def _notify_user(self, user_id: int) -> None:
        text = "✅ Оплата получена\n\nТариф BASIC активирован.\nСпасибо, что выбрали ViMi."
        if self._notifier:
            self._notifier(user_id, text)
            return
        if not settings.bot_token:
            LOGGER.warning("Не удалось отправить уведомление: BOT_TOKEN пуст user_id=%s", user_id)
            return

        async def _send() -> None:
            bot = Bot(token=settings.bot_token)
            try:
                from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💎 Моя подписка", callback_data="user_subscription")],
                        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_main")],
                    ]
                )
                await bot.send_message(chat_id=user_id, text=text, reply_markup=kb)
            finally:
                await bot.session.close()

        import asyncio

        try:
            asyncio.run(_send())
        except Exception as exc:
            LOGGER.warning("Ошибка отправки Telegram-уведомления об оплате user_id=%s error=%s", user_id, exc)

    def _record_event(self, *, tenant_id: int, event_type: str, metadata: dict[str, Any]) -> None:
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(int(tenant_id), event_type, event_source="lava_webhook", metadata=metadata)

    @staticmethod
    def _fallback_event_id(*, raw_body: str, provider_invoice_id: str, status: str) -> str:
        source = f"{provider_invoice_id}:{status}:{raw_body}"
        return hashlib.sha256(source.encode("utf-8")).hexdigest()
