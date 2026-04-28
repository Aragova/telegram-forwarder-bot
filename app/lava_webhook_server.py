from __future__ import annotations

import json
import logging

from aiohttp import web

from app.payments.lava_webhook_activation import LavaWebhookActivationService, verify_lava_webhook_auth
from app.repository_factory import create_repository

LOGGER = logging.getLogger("forwarder.payments.lava.webhook.http")


async def lava_webhook(request: web.Request) -> web.Response:
    raw_body = await request.text()
    headers = {k: v for k, v in request.headers.items()}
    auth = verify_lava_webhook_auth(headers, raw_body)
    if not auth.ok:
        LOGGER.warning("Lava webhook auth failed reason=%s", auth.reason)
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        payload = json.loads(raw_body) if raw_body else {}
    except Exception:
        LOGGER.warning("Lava webhook invalid json")
        return web.json_response({"ok": True, "status": "invalid_json_ignored"}, status=202)

    if not isinstance(payload, dict):
        return web.json_response({"ok": True, "status": "invalid_payload_ignored"}, status=202)

    repo = create_repository()
    service = LavaWebhookActivationService(repo)
    result = service.process_webhook(payload, raw_body)
    return web.json_response({"ok": result.ok, "code": result.code}, status=result.http_status)


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/payments/lava/webhook", lava_webhook)
    return app


def main() -> None:
    web.run_app(create_app(), host="0.0.0.0", port=8089)


if __name__ == "__main__":
    main()
