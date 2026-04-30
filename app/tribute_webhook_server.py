from __future__ import annotations

import logging

from aiohttp import web

from app.payment_service import PaymentService
from app.repository_factory import create_repository

LOGGER = logging.getLogger("forwarder.payments.tribute")


async def tribute_webhook(request: web.Request) -> web.Response:
    raw_body = await request.text()
    headers = {k: v for k, v in request.headers.items()}
    try:
        service = PaymentService(create_repository())
        result = service.handle_provider_webhook("tribute", headers, raw_body)
    except Exception as exc:
        LOGGER.exception("Tribute webhook internal error: %s", exc)
        return web.json_response({"ok": False, "error": "internal_error"}, status=500)
    if result.get("ok"):
        return web.json_response(result, status=200)
    error = str(result.get("error") or "")
    if error == "invalid_signature":
        return web.json_response(result, status=401)
    if error == "bad_json":
        return web.json_response(result, status=400)
    if error == "payment_intent_not_found":
        return web.json_response(result, status=404)
    return web.json_response(result, status=200)


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/payments/tribute/webhook", tribute_webhook)
    return app


def main() -> None:
    web.run_app(create_app(), host="0.0.0.0", port=8090)


if __name__ == "__main__":
    main()
