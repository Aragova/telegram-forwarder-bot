from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.config import settings

LOGGER = logging.getLogger("forwarder.payments.tribute")


class TributeAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class TributeClient:
    def __init__(self) -> None:
        self._api_base = str(settings.tribute_api_base or "https://tribute.tg").rstrip("/")
        self._api_key = str(settings.tribute_api_key or "")
        self._timeout = float(settings.tribute_request_timeout_sec or 20)

    def create_shop_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/api/v1/shop/orders", json_payload=payload)

    def get_shop_order(self, order_uuid: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/shop/orders/{order_uuid}/status")

    def cancel_shop_order(self, order_uuid: str) -> dict[str, Any]:
        return self._request("POST", f"/api/v1/shop/orders/{order_uuid}/cancel")

    def _request(self, method: str, path: str, json_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._api_base}{path}"
        if not self._api_key:
            raise TributeAPIError("tribute_api_key_missing")
        headers = {"Api-Key": self._api_key, "Content-Type": "application/json"}
        LOGGER.info("Tribute API request method=%s path=%s", method, path)
        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.request(method, url, headers=headers, json=json_payload)
        except httpx.TimeoutException as exc:
            raise TributeAPIError("tribute_api_timeout") from exc
        except httpx.HTTPError as exc:
            raise TributeAPIError("tribute_api_request_error") from exc
        LOGGER.info("Tribute API response path=%s status_code=%s", path, response.status_code)
        if response.status_code >= 400:
            snippet = (response.text or "")[:300]
            if self._api_key:
                snippet = snippet.replace(self._api_key, "***")
            raise TributeAPIError(f"tribute_api_http_error status={response.status_code} body={snippet}", status_code=response.status_code)
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            snippet = (response.text or "")[:300]
            if self._api_key:
                snippet = snippet.replace(self._api_key, "***")
            raise TributeAPIError(f"tribute_api_bad_json status={response.status_code} body={snippet}", status_code=response.status_code) from exc
        if not isinstance(data, dict):
            raise TributeAPIError("Tribute API вернул неожиданный формат ответа", status_code=response.status_code)
        return data
