from __future__ import annotations

import html
import logging

from aiohttp import web

from app.config import settings
from app.reaction_auth_service import ReactionAuthService
from app.reaction_onboarding_token import verify_reaction_onboarding_token
from app.repository_factory import create_repository

LOGGER = logging.getLogger("forwarder.reaction.onboarding.http")

SAFE_TOKEN_ERROR_TEXT = "Ссылка подключения недействительна или устарела. Откройте подключение заново из Telegram-бота."
SAFE_UNEXPECTED_ERROR_TEXT = "Не удалось открыть подключение. Попробуйте заново из Telegram-бота."


def _layout(title: str, body: str) -> str:
    return f"""<!doctype html><html lang='ru'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{html.escape(title)}</title><style>body{{font-family:Arial,sans-serif;background:#f6f7fb;margin:0}}.c{{max-width:560px;margin:40px auto;background:#fff;border-radius:12px;padding:24px}}input,button{{width:100%;padding:12px;margin-top:10px}}.w{{background:#fff7e6;padding:12px;border-radius:8px}}</style></head><body><div class='c'>{body}</div></body></html>"""


def _error_page(msg: str) -> web.Response:
    body = f"<h1>Ошибка подключения</h1><p>{html.escape(msg)}</p>"
    return web.Response(text=_layout("Ошибка", body), content_type="text/html")


def _token_payload(token: str) -> dict:
    _ensure_onboarding_available()
    return verify_reaction_onboarding_token(token, secret=settings.reaction_onboarding_secret)


def _ensure_onboarding_available() -> None:
    if not settings.reaction_onboarding_enabled:
        raise ValueError("Защищённое подключение временно отключено")
    if not settings.public_base_url or not settings.reaction_onboarding_secret:
        raise ValueError("Защищённая страница подключения пока не включена")


def _phone_page(token: str) -> web.Response:
    body = (
        "<h1>Подключение аккаунта-реактора</h1>"
        "<p>Введите номер Telegram-аккаунта, который принадлежит вам или вашей команде.</p>"
        "<div class='w'>Никогда не отправляйте Telegram login-code в чат бота. На этой защищённой странице код используется только для создания вашей сессии подключения.</div>"
        f"<form method='post' action='{settings.reaction_onboarding_public_path}/phone'>"
        f"<input type='hidden' name='token' value='{html.escape(token)}'>"
        "<input name='phone' placeholder='+79991234567' required>"
        "<button type='submit'>Отправить код</button></form>"
    )
    return web.Response(text=_layout("Подключение", body), content_type="text/html")


def _password_page(token: str, phone: str, error_text: str | None = None) -> web.Response:
    error_html = f"<div class='w'>{html.escape(error_text)}</div>" if error_text else ""
    body = (
        "<h1>Требуется 2FA</h1>"
        f"{error_html}"
        "<p>Введите пароль двухэтапной защиты. Пароль не сохраняется.</p>"
        f"<form method='post' action='{settings.reaction_onboarding_public_path}/password'>"
        f"<input type='hidden' name='token' value='{html.escape(token)}'>"
        f"<input type='hidden' name='phone' value='{html.escape(phone)}'>"
        "<input name='password' type='password' required>"
        "<button type='submit'>Завершить подключение</button></form>"
    )
    return web.Response(text=_layout("2FA", body), content_type="text/html")


async def page_open(request: web.Request) -> web.Response:
    token = request.query.get("token", "")
    try:
        payload = _token_payload(token)
    except ValueError:
        return _error_page(SAFE_TOKEN_ERROR_TEXT)
    except Exception:
        return _error_page(SAFE_UNEXPECTED_ERROR_TEXT)
    LOGGER.info("REACTION_ONBOARDING_PAGE_OPENED | tenant_id=%s | rule_id=%s | user_id=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"])
    return _phone_page(token)


async def phone_submit(request: web.Request) -> web.Response:
    data = await request.post()
    token = str(data.get("token") or "")
    phone = str(data.get("phone") or "")
    try:
        payload = _token_payload(token)
        service: ReactionAuthService = request.app["auth_service"]
        result = await service.start_phone_login(tenant_id=payload["tenant_id"], rule_id=payload["rule_id"], user_id=payload["user_id"], phone=phone)
        LOGGER.info("REACTION_ONBOARDING_CODE_SENT | tenant_id=%s | rule_id=%s | user_id=%s | phone_hint=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], result["phone_hint"])
        body = (
            "<h1>Код отправлен</h1>"
            f"<p>Введите код, который пришёл в Telegram на аккаунт {html.escape(result['phone_hint'])}.</p>"
            f"<form method='post' action='{settings.reaction_onboarding_public_path}/code'>"
            f"<input type='hidden' name='token' value='{html.escape(token)}'>"
            f"<input type='hidden' name='phone' value='{html.escape(result['phone'])}'>"
            f"<input type='hidden' name='phone_code_hash' value='{html.escape(result['phone_code_hash'])}'>"
            "<input name='code' placeholder='12345' required>"
            "<button type='submit'>Подключить</button></form>"
        )
        return web.Response(text=_layout("Код", body), content_type="text/html")
    except Exception as exc:
        LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=%s", exc.__class__.__name__)
        return _error_page(str(exc) if isinstance(exc, ValueError) else "Не удалось отправить код")


async def code_submit(request: web.Request) -> web.Response:
    data = await request.post()
    token = str(data.get("token") or "")
    try:
        payload = _token_payload(token)
        service: ReactionAuthService = request.app["auth_service"]
        result = await service.complete_code_login(
            tenant_id=payload["tenant_id"], rule_id=payload["rule_id"], user_id=payload["user_id"], phone=str(data.get("phone") or ""), phone_code_hash=str(data.get("phone_code_hash") or ""), code=str(data.get("code") or "")
        )
        if result.get("status") == "password_required":
            mask = service.mask_phone(str(data.get("phone") or ""))
            LOGGER.info("REACTION_ONBOARDING_PASSWORD_REQUIRED | tenant_id=%s | rule_id=%s | user_id=%s | phone_hint=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], mask)
            return _password_page(token, str(data.get("phone") or ""))
        LOGGER.info("REACTION_ONBOARDING_SUCCESS | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | telegram_user_id=%s | is_premium=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], result.get("account_id"), result.get("telegram_user_id"), result.get("is_premium"))
        ident = f"@{result.get('username')}" if result.get("username") else f"ID {result.get('telegram_user_id')}"
        body = f"<h1>Аккаунт-реактор подключён</h1><p>{html.escape(ident)}</p><p>Premium: {'да' if result.get('is_premium') else 'нет'}</p><p>{html.escape(result.get('phone_hint') or '')}</p><p><a href='https://t.me/topposter69_bot'>Вернуться в Telegram-бот</a></p>"
        return web.Response(text=_layout("Успех", body), content_type="text/html")
    except Exception as exc:
        LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=%s", exc.__class__.__name__)
        return _error_page(str(exc) if isinstance(exc, ValueError) else "Не удалось завершить вход")


async def password_submit(request: web.Request) -> web.Response:
    data = await request.post()
    token = str(data.get("token") or "")
    try:
        payload = _token_payload(token)
        service: ReactionAuthService = request.app["auth_service"]
        result = await service.complete_password_login(tenant_id=payload["tenant_id"], rule_id=payload["rule_id"], user_id=payload["user_id"], password=str(data.get("password") or ""), phone=str(data.get("phone") or ""))
        LOGGER.info("REACTION_ONBOARDING_SUCCESS | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | telegram_user_id=%s | is_premium=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], result.get("account_id"), result.get("telegram_user_id"), result.get("is_premium"))
        ident = f"@{result.get('username')}" if result.get("username") else f"ID {result.get('telegram_user_id')}"
        body = f"<h1>Аккаунт-реактор подключён</h1><p>{html.escape(ident)}</p><p>Premium: {'да' if result.get('is_premium') else 'нет'}</p><p>{html.escape(result.get('phone_hint') or '')}</p><p><a href='https://t.me/topposter69_bot'>Вернуться в Telegram-бот</a></p>"
        return web.Response(text=_layout("Успех", body), content_type="text/html")
    except ValueError as exc:
        if str(exc) == "Неверный 2FA-пароль. Проверьте пароль и попробуйте снова.":
            LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=ValueError | reason=password_invalid")
            return _password_page(token, str(data.get("phone") or ""), error_text=str(exc))
        LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=ValueError")
        return _error_page(str(exc))
    except Exception as exc:
        LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=%s", exc.__class__.__name__)
        return _error_page("Не удалось завершить вход. Попробуйте заново из Telegram-бота.")


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def create_app() -> web.Application:
    app = web.Application()
    repo = create_repository()
    app["auth_service"] = ReactionAuthService(repo, api_id=settings.api_id, api_hash=settings.api_hash)
    app.router.add_get(settings.reaction_onboarding_public_path, page_open)
    app.router.add_get(f"{settings.reaction_onboarding_public_path}/", page_open)
    app.router.add_post(f"{settings.reaction_onboarding_public_path}/phone", phone_submit)
    app.router.add_post(f"{settings.reaction_onboarding_public_path}/code", code_submit)
    app.router.add_post(f"{settings.reaction_onboarding_public_path}/password", password_submit)
    app.router.add_get(f"{settings.reaction_onboarding_public_path}/health", health)
    return app


def main() -> None:
    web.run_app(create_app(), host="0.0.0.0", port=settings.reaction_onboarding_port)


if __name__ == "__main__":
    main()
