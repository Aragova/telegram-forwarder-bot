from __future__ import annotations

import asyncio

import pytest

httpx = pytest.importorskip("httpx")

from app.payments.lava_top_client import LavaTopAPIError, LavaTopClient


def test_create_invoice_posts_to_expected_endpoint_and_normalizes_response(monkeypatch):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        captured["json"] = __import__("json").loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "id": "inv_123",
                "status": "new",
                "amountTotal": {"amount": 9.0, "currency": "USD"},
                "paymentUrl": "https://gate.lava.top/pay/abc",
            },
        )

    original_async_client = httpx.AsyncClient

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self._client = original_async_client(transport=httpx.MockTransport(handler), timeout=kwargs.get("timeout"))

        async def __aenter__(self):
            return self._client

        async def __aexit__(self, exc_type, exc, tb):
            await self._client.aclose()

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)
    client = LavaTopClient(api_key="secret-key", api_base="https://gate.lava.top")
    result = asyncio.run(
        client.create_invoice(
            email="u@example.com",
            offer_id="offer-1",
            currency="usd",
            buyer_language="ru",
            client_order_id="order-123",
        )
    )

    assert captured["url"] == "https://gate.lava.top/api/v3/invoice"
    assert captured["json"]["buyerLanguage"] == "RU"
    assert captured["json"]["currency"] == "USD"
    assert captured["json"]["clientOrderId"] == "order-123"
    assert result.invoice_id == "inv_123"
    assert result.status == "new"
    assert result.amount == 9.0
    assert result.currency == "USD"
    assert result.payment_url == "https://gate.lava.top/pay/abc"


def test_http_400_becomes_lava_error_without_api_key_leak(monkeypatch):
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"message": "bad request"})

    original_async_client = httpx.AsyncClient

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self._client = original_async_client(transport=httpx.MockTransport(handler), timeout=kwargs.get("timeout"))

        async def __aenter__(self):
            return self._client

        async def __aexit__(self, exc_type, exc, tb):
            await self._client.aclose()

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)
    client = LavaTopClient(api_key="super-secret-api-key", api_base="https://gate.lava.top")

    try:
        asyncio.run(client.create_invoice(email="u@example.com", offer_id="offer-1"))
    except LavaTopAPIError as exc:
        error_text = str(exc)
        assert "HTTP 400" in error_text
        assert "super-secret-api-key" not in error_text
    else:
        assert False, "Expected LavaTopAPIError"
