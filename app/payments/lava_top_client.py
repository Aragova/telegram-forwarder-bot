from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover
    httpx = None

from app.config import settings


class LavaTopAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, details: str | None = None) -> None:
        self.status_code = status_code
        self.details = details
        super().__init__(message)


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

        amount_total = raw.get("amountTotal") if isinstance(raw.get("amountTotal"), dict) else {}
        return LavaTopInvoiceResult(
            invoice_id=str(raw.get("id") or ""),
            status=str(raw.get("status") or ""),
            amount=float(amount_total.get("amount") or 0),
            currency=str(amount_total.get("currency") or payload["currency"]).upper(),
            payment_url=str(raw.get("paymentUrl") or "").strip(),
            raw=raw,
        )
