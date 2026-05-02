from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(raw: str) -> bytes:
    value = str(raw or "").strip()
    pad = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + pad)


def create_reaction_onboarding_token(
    *,
    tenant_id: int,
    user_id: int,
    rule_id: int,
    secret: str,
    ttl_sec: int,
) -> str:
    if not secret:
        raise ValueError("Onboarding token secret не настроен")
    payload = {
        "tenant_id": int(tenant_id),
        "user_id": int(user_id),
        "rule_id": int(rule_id),
        "exp": int(time.time()) + max(int(ttl_sec), 1),
        "nonce": secrets.token_urlsafe(16),
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(secret.encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url_encode(sig)}"


def verify_reaction_onboarding_token(token: str, *, secret: str) -> dict:
    if not secret:
        raise ValueError("Onboarding token secret не настроен")
    try:
        payload_part, sig_part = str(token or "").split(".", 1)
        expected_sig = hmac.new(secret.encode("utf-8"), payload_part.encode("ascii"), hashlib.sha256).digest()
        got_sig = _b64url_decode(sig_part)
        if not hmac.compare_digest(expected_sig, got_sig):
            raise ValueError("Некорректный токен подключения")
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Некорректный токен подключения")
        if int(payload.get("exp") or 0) < int(time.time()):
            raise ValueError("Ссылка подключения истекла. Откройте подключение заново из бота")
        for key in ("tenant_id", "user_id", "rule_id"):
            payload[key] = int(payload.get(key) or 0)
        return payload
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Некорректный токен подключения") from exc
