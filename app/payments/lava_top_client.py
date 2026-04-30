from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover
    httpx = None

from app.config import settings

LOGGER = logging.getLogger("forwarder.payments.lava.client")


class LavaTopAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, details: str | None = None) -> None:
        self.status_code = status_code
        self.details = details
        super().__init__(message)




_SENSITIVE_KEYS = {"apikey", "api_key", "token", "accesstoken", "access_token", "authorization", "signature", "secret", "password", "paymenturl", "checkouturl"}


def _looks_like_payment_url(value: str) -> bool:
    lower = value.lower()
    return lower.startswith(("http://", "https://")) and any(mark in lower for mark in ("checkout", "payment", "/pay", "invoice"))


def sanitize_lava_response_for_log(data: Any, *, _depth: int = 0, _max_depth: int = 4, _max_items: int = 20) -> Any:
    if _depth >= _max_depth:
        return "<truncated_depth>"

    if isinstance(data, dict):
        out: dict[str, Any] = {}
        items = list(data.items())
        for idx, (key, value) in enumerate(items):
            if idx >= _max_items:
                out["__truncated_items__"] = f"{len(items) - _max_items} more"
                break
            key_str = str(key)
            key_norm = key_str.lower().replace("-", "").replace("_", "")
            if key_norm in _SENSITIVE_KEYS:
                out[key_str] = "<masked>" if "url" not in key_norm else "<masked_url>"
                continue
            if key_norm == "url" and isinstance(value, str) and _looks_like_payment_url(value):
                out[key_str] = "<masked_url>"
                continue
            out[key_str] = sanitize_lava_response_for_log(value, _depth=_depth + 1, _max_depth=_max_depth, _max_items=_max_items)
        return out

    if isinstance(data, list):
        if len(data) > _max_items:
            return [sanitize_lava_response_for_log(v, _depth=_depth + 1, _max_depth=_max_depth, _max_items=_max_items) for v in data[:_max_items]] + [f"<truncated_items:{len(data)-_max_items}>"]
        return [sanitize_lava_response_for_log(v, _depth=_depth + 1, _max_depth=_max_depth, _max_items=_max_items) for v in data]

    if isinstance(data, str) and _looks_like_payment_url(data):
        return "<masked_url>"

    return data


@dataclass(frozen=True, slots=True)
class LavaTopInvoiceResult:
    invoice_id: str
    status: str
    amount: float
    currency: str
    payment_url: str
    raw: dict[str, Any]


class LavaTopClient:
    def __init__(self, *, api_key: str | None = None, api_base: str | None = None) -> None:
        self._api_key = (api_key if api_key is not None else settings.lava_top_api_key).strip()
        self._api_base = (api_base if api_base is not None else settings.lava_top_api_base).rstrip("/")

    async def create_invoice(
        self,
        *,
        email: str,
        offer_id: str,
        currency: str = "USD",
        buyer_language: str = "RU",
        client_order_id: str | None = None,
        payment_provider: str | None = None,
        payment_method: str | None = None,
    ) -> LavaTopInvoiceResult:
        if httpx is None:
            raise LavaTopAPIError("Не установлен пакет httpx для работы Lava.top")

        if not self._api_key:
            raise LavaTopAPIError("Не задан LAVA_TOP_API_KEY", details="missing_api_key")

        payload: dict[str, Any] = {
            "email": str(email or "").strip(),
            "offerId": str(offer_id or "").strip(),
            "currency": str(currency or "USD").upper(),
            "buyerLanguage": str(buyer_language or "RU").upper(),
        }
        if client_order_id:
            payload["clientOrderId"] = str(client_order_id)
        if payment_provider:
            payload["paymentProvider"] = str(payment_provider)
        if payment_method:
            payload["paymentMethod"] = str(payment_method)

        headers = {
            "X-Api-Key": self._api_key,
            "Content-Type": "application/json",
        }
        endpoint = f"{self._api_base}/api/v3/invoice"

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(endpoint, json=payload, headers=headers)

        if response.status_code >= 400:
            details = ""
            try:
                body = response.json()
                details = str(body.get("message") or body.get("error") or body)[:500]
            except Exception:
                details = (response.text or "")[:500]
            raise LavaTopAPIError(
                f"Ошибка Lava.top при создании счёта (HTTP {response.status_code})",
                status_code=response.status_code,
                details=details,
            )

        try:
            raw = response.json()
        except Exception as exc:
            raise LavaTopAPIError("Lava.top вернул некорректный JSON") from exc
        if not isinstance(raw, dict):
            raise LavaTopAPIError("Lava.top вернул неожиданный формат ответа")

        keys = sorted(raw.keys())
        contract_id = raw.get("contractId")
        parent_contract_id = raw.get("ParentContractId") if raw.get("ParentContractId") is not None else raw.get("parentContractId")
        payment_url = str(raw.get("paymentUrl") or raw.get("checkoutUrl") or "").strip()
        LOGGER.info(
            "LAVA_CREATE_INVOICE_RESPONSE_KEYS | keys=%s | contractId_present=%s | parentContractId_present=%s | payment_url_present=%s | invoice_id_present=%s | status=%s",
            keys,
            contract_id is not None,
            parent_contract_id is not None,
            bool(payment_url),
            bool(raw.get("id") or raw.get("invoiceId")),
            raw.get("status"),
        )
        LOGGER.debug("LAVA_CREATE_INVOICE_RESPONSE_SAFE | payload=%s", sanitize_lava_response_for_log(raw))

        amount_total = raw.get("amountTotal") if isinstance(raw.get("amountTotal"), dict) else {}
        return LavaTopInvoiceResult(
            invoice_id=str(raw.get("id") or ""),
            status=str(raw.get("status") or ""),
            amount=float(amount_total.get("amount") or 0),
            currency=str(amount_total.get("currency") or payload["currency"]).upper(),
            payment_url=payment_url,
            raw=raw,
        )
