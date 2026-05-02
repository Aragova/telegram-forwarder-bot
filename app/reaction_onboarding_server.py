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


TEXTS = {
    "ru": {
        "back": "← Назад на сайт",
        "security": "Никогда не отправляйте Telegram login-code в чат бота. На этой защищённой странице код используется только для создания сессии подключения.",
        "connect_title": "Подключение аккаунта-реактора",
        "connect_desc": "Введите номер Telegram-аккаунта, который принадлежит вам или вашей команде.",
        "phone_ph": "+79991234567",
        "send_code": "Отправить код",
        "code_title": "Код отправлен",
        "code_desc": "Введите код, который пришёл в Telegram на аккаунт {phone_hint}.",
        "code_ph": "12345",
        "connect_btn": "Подключить",
        "password_title": "Требуется 2FA",
        "password_desc": "Введите пароль двухэтапной защиты. Пароль не сохраняется.",
        "password_btn": "Завершить подключение",
        "success_title": "Аккаунт-реактор подключён",
        "premium": "Premium: {value}",
        "yes": "да",
        "no": "нет",
        "open_accounts": "Открыть мои аккаунты-реакторы",
        "recovered": "Подключение уже завершено. Мы восстановили экран успеха после технического сбоя отображения.",
        "error_title": "Ошибка подключения",
        "lang_ru": "RU",
        "lang_en": "EN",
        "brand": "ViMi",
    },
    "en": {
        "back": "← Back to website",
        "security": "Never send your Telegram login code in bot chat. On this secure page, the code is used only to create your connection session.",
        "connect_title": "Reactor account connection",
        "connect_desc": "Enter the Telegram account phone number that belongs to you or your team.",
        "phone_ph": "+15551234567",
        "send_code": "Send code",
        "code_title": "Code sent",
        "code_desc": "Enter the code sent in Telegram to account {phone_hint}.",
        "code_ph": "12345",
        "connect_btn": "Connect",
        "password_title": "2FA required",
        "password_desc": "Enter your two-step verification password. The password is not stored.",
        "password_btn": "Finish connection",
        "success_title": "Reactor account connected",
        "premium": "Premium: {value}",
        "yes": "yes",
        "no": "no",
        "open_accounts": "Open my reactor accounts",
        "recovered": "Connection is already completed. We restored the success screen after a technical rendering issue.",
        "error_title": "Connection error",
        "lang_ru": "RU",
        "lang_en": "EN",
        "brand": "ViMi",
    },
}


def _get_lang(request: web.Request | None = None, data: dict | None = None) -> str:
    lang = ""
    if request is not None:
        lang = str(request.query.get("lang") or "")
    if not lang and data is not None:
        lang = str(data.get("lang") or "")
    return "en" if lang == "en" else "ru"


def _layout(title: str, body: str, lang: str, token: str = "") -> str:
    t = TEXTS[lang]
    ru_link = f"{settings.reaction_onboarding_public_path}?token={html.escape(token)}&lang=ru" if token else f"{settings.reaction_onboarding_public_path}?lang=ru"
    en_link = f"{settings.reaction_onboarding_public_path}?token={html.escape(token)}&lang=en" if token else f"{settings.reaction_onboarding_public_path}?lang=en"
    return f"""<!doctype html><html lang='{lang}'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{html.escape(title)}</title><style>
:root{{--bg1:#0f172a;--bg2:#111827;--card:#ffffff;--txt:#111827;--muted:#6b7280;--accent:#7c3aed;--accent2:#ec4899;--warn:#fff7e6;--warnb:#f59e0b}}
*{{box-sizing:border-box}}body{{font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;background:radial-gradient(circle at 20% 10%,#312e81 0%,transparent 40%),radial-gradient(circle at 80% 90%,#9d174d 0%,transparent 35%),linear-gradient(135deg,var(--bg1),var(--bg2));margin:0;min-height:100vh;color:var(--txt)}}
.wrap{{max-width:640px;margin:24px auto;padding:16px}}.top{{display:flex;justify-content:space-between;align-items:center;color:#fff;margin-bottom:14px}}.brand{{font-weight:700;letter-spacing:.4px}}.lang a{{color:#fff;text-decoration:none;opacity:.8;margin-left:10px}}.lang a.active{{opacity:1;text-decoration:underline}}
.card{{background:var(--card);border-radius:16px;padding:24px;box-shadow:0 18px 45px rgba(0,0,0,.32)}}h1{{margin:0 0 10px;font-size:26px}}p{{color:var(--muted)}}
input,button{{width:100%;padding:13px 14px;margin-top:10px;border-radius:10px;font-size:16px}}input{{border:1px solid #d1d5db}}button{{border:none;background:linear-gradient(90deg,var(--accent),var(--accent2));color:#fff;font-weight:600;cursor:pointer}}
.warn{{background:var(--warn);border:1px solid #fde68a;padding:12px;border-radius:10px;color:#92400e;margin:14px 0}}.back{{display:inline-block;margin-top:12px;color:#4f46e5;text-decoration:none;font-weight:500}}
@media (max-width:640px){{.wrap{{margin:10px auto}}.card{{padding:18px}}h1{{font-size:22px}}}}
</style></head><body><div class='wrap'><div class='top'><div class='brand'>{t['brand']}</div><div class='lang'><a class='{'active' if lang=='ru' else ''}' href='{ru_link}'>{t['lang_ru']}</a><a class='{'active' if lang=='en' else ''}' href='{en_link}'>{t['lang_en']}</a></div></div><div class='card'>{body}<a class='back' href='https://usevimi.ru/'>{t['back']}</a></div></div></body></html>"""


def _error_page(msg: str, lang: str = "ru") -> web.Response:
    t = TEXTS[lang]
    body = f"<h1>{t['error_title']}</h1><p>{html.escape(msg)}</p>"
    return web.Response(text=_layout(t["error_title"], body, lang), content_type="text/html")


def _success_page(result: dict, *, recovered: bool = False, lang: str = "ru") -> web.Response:
    t = TEXTS[lang]
    ident = f"@{result.get('username')}" if result.get("username") else f"ID {result.get('telegram_user_id')}"
    body = (
        f"<h1>{t['success_title']}</h1>"
        f"<p>{html.escape(ident)}</p>"
        f"<p>{html.escape(t['premium'].format(value=(t['yes'] if result.get('is_premium') else t['no'])))}</p>"
        f"<p>{html.escape(result.get('phone_hint') or '')}</p>"
    )
    if recovered:
        body += f"<div class='warn'>{html.escape(t['recovered'])}</div>"
    rule_id = result.get("rule_id")
    bot_url = f"https://t.me/topposter69_bot?start=reaction_accounts_{int(rule_id)}" if rule_id is not None else "https://t.me/topposter69_bot"
    body += f"<p><a href='{bot_url}'>{t['open_accounts']}</a></p>"
    return web.Response(text=_layout(t["success_title"], body, lang), content_type="text/html")


def _recover_success_result(request: web.Request, payload: dict) -> dict | None:
    repo = request.app.get("repo")
    if not repo:
        return None

    tenant_id = int(payload["tenant_id"])
    telegram_user_id = int(payload["user_id"])

    account = repo.get_active_reaction_account_by_telegram_user_for_tenant(
        tenant_id=tenant_id,
        telegram_user_id=telegram_user_id,
    )

    if not account:
        return None

    return {
        "status": "success",
        "account_id": account.get("id"),
        "telegram_user_id": account.get("telegram_user_id"),
        "username": account.get("username"),
        "is_premium": account.get("is_premium"),
        "phone_hint": account.get("phone_hint"),
        "tenant_id": payload["tenant_id"],
        "rule_id": payload["rule_id"],
        "user_id": payload["user_id"],
        "recovered_after_error": True,
    }


def _token_payload(token: str) -> dict:
    _ensure_onboarding_available()
    return verify_reaction_onboarding_token(token, secret=settings.reaction_onboarding_secret)


def _ensure_onboarding_available() -> None:
    if not settings.reaction_onboarding_enabled:
        raise ValueError("Защищённое подключение временно отключено")
    if not settings.public_base_url or not settings.reaction_onboarding_secret:
        raise ValueError("Защищённая страница подключения пока не включена")


def _phone_page(token: str, lang: str) -> web.Response:
    t = TEXTS[lang]
    body = (
        f"<h1>{t['connect_title']}</h1>"
        f"<p>{t['connect_desc']}</p>"
        f"<div class='warn'>{t['security']}</div>"
        f"<form method='post' action='{settings.reaction_onboarding_public_path}/phone'>"
        f"<input type='hidden' name='token' value='{html.escape(token)}'>"
        f"<input type='hidden' name='lang' value='{lang}'>"
        f"<input name='phone' type='tel' placeholder='{t['phone_ph']}' required>"
        f"<button type='submit'>{t['send_code']}</button></form>"
    )
    return web.Response(text=_layout(t["connect_title"], body, lang, token), content_type="text/html")


def _password_page(token: str, phone: str, lang: str, error_text: str | None = None) -> web.Response:
    t = TEXTS[lang]
    error_html = f"<div class='warn'>{html.escape(error_text)}</div>" if error_text else ""
    body = (
        f"<h1>{t['password_title']}</h1>"
        f"{error_html}"
        f"<p>{t['password_desc']}</p>"
        f"<form method='post' action='{settings.reaction_onboarding_public_path}/password'>"
        f"<input type='hidden' name='token' value='{html.escape(token)}'>"
        f"<input type='hidden' name='lang' value='{lang}'>"
        f"<input type='hidden' name='phone' value='{html.escape(phone)}'>"
        "<input name='password' type='password' required>"
        f"<button type='submit'>{t['password_btn']}</button></form>"
    )
    return web.Response(text=_layout(t["password_title"], body, lang, token), content_type="text/html")


async def page_open(request: web.Request) -> web.Response:
    token = request.query.get("token", "")
    lang = _get_lang(request=request)
    try:
        payload = _token_payload(token)
    except ValueError:
        return _error_page(SAFE_TOKEN_ERROR_TEXT, lang)
    except Exception:
        return _error_page(SAFE_UNEXPECTED_ERROR_TEXT, lang)
    LOGGER.info("REACTION_ONBOARDING_PAGE_OPENED | tenant_id=%s | rule_id=%s | user_id=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"])
    return _phone_page(token, lang)


async def phone_submit(request: web.Request) -> web.Response:
    data = await request.post()
    token = str(data.get("token") or "")
    phone = str(data.get("phone") or "")
    lang = _get_lang(data=data)
    try:
        payload = _token_payload(token)
        service: ReactionAuthService = request.app["auth_service"]
        result = await service.start_phone_login(tenant_id=payload["tenant_id"], rule_id=payload["rule_id"], user_id=payload["user_id"], phone=phone)
        LOGGER.info("REACTION_ONBOARDING_CODE_SENT | tenant_id=%s | rule_id=%s | user_id=%s | phone_hint=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], result["phone_hint"])
        t = TEXTS[lang]
        body = (
            f"<h1>{t['code_title']}</h1>"
            f"<p>{t['code_desc'].format(phone_hint=html.escape(result['phone_hint']))}</p>"
            f"<div class='warn'>{t['security']}</div>"
            f"<form method='post' action='{settings.reaction_onboarding_public_path}/code'>"
            f"<input type='hidden' name='token' value='{html.escape(token)}'>"
            f"<input type='hidden' name='lang' value='{lang}'>"
            f"<input type='hidden' name='phone' value='{html.escape(result['phone'])}'>"
            f"<input type='hidden' name='phone_code_hash' value='{html.escape(result['phone_code_hash'])}'>"
            f"<input name='code' placeholder='{t['code_ph']}' required>"
            f"<button type='submit'>{t['connect_btn']}</button></form>"
        )
        return web.Response(text=_layout(t["code_title"], body, lang, token), content_type="text/html")
    except Exception as exc:
        LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=%s", exc.__class__.__name__)
        return _error_page(str(exc) if isinstance(exc, ValueError) else "Не удалось отправить код", lang)


async def code_submit(request: web.Request) -> web.Response:
    data = await request.post()
    token = str(data.get("token") or "")
    lang = _get_lang(data=data)
    try:
        payload = _token_payload(token)
        service: ReactionAuthService = request.app["auth_service"]
        result = await service.complete_code_login(
            tenant_id=payload["tenant_id"], rule_id=payload["rule_id"], user_id=payload["user_id"], phone=str(data.get("phone") or ""), phone_code_hash=str(data.get("phone_code_hash") or ""), code=str(data.get("code") or "")
        )
        if result.get("status") == "password_required":
            mask = service.mask_phone(str(data.get("phone") or ""))
            LOGGER.info("REACTION_ONBOARDING_PASSWORD_REQUIRED | tenant_id=%s | rule_id=%s | user_id=%s | phone_hint=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], mask)
            return _password_page(token, str(data.get("phone") or ""), lang)
        LOGGER.info("REACTION_ONBOARDING_SUCCESS | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | telegram_user_id=%s | is_premium=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], result.get("account_id"), result.get("telegram_user_id"), result.get("is_premium"))
        result["tenant_id"] = payload["tenant_id"]
        result["rule_id"] = payload["rule_id"]
        result["user_id"] = payload["user_id"]
        return _success_page(result, lang=lang)
    except ValueError as exc:
        LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=ValueError")
        return _error_page(str(exc), lang)
    except Exception as exc:
        LOGGER.exception(
            "REACTION_ONBOARDING_FAILED | tenant_id=%s | rule_id=%s | user_id=%s | error_type=%s",
            payload.get("tenant_id") if 'payload' in locals() else None,
            payload.get("rule_id") if 'payload' in locals() else None,
            payload.get("user_id") if 'payload' in locals() else None,
            exc.__class__.__name__,
        )
        if 'payload' in locals():
            recovered = _recover_success_result(request, payload)
            if recovered:
                LOGGER.warning(
                    "REACTION_ONBOARDING_SUCCESS_RECOVERED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | original_error_type=%s",
                    payload.get("tenant_id"),
                    payload.get("rule_id"),
                    payload.get("user_id"),
                    recovered.get("account_id"),
                    exc.__class__.__name__,
                )
                return _success_page(recovered, recovered=True, lang=lang)
        return _error_page("Не удалось завершить вход", lang)


async def password_submit(request: web.Request) -> web.Response:
    data = await request.post()
    token = str(data.get("token") or "")
    lang = _get_lang(data=data)
    try:
        payload = _token_payload(token)
        service: ReactionAuthService = request.app["auth_service"]
        result = await service.complete_password_login(tenant_id=payload["tenant_id"], rule_id=payload["rule_id"], user_id=payload["user_id"], password=str(data.get("password") or ""), phone=str(data.get("phone") or ""))
        LOGGER.info("REACTION_ONBOARDING_SUCCESS | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | telegram_user_id=%s | is_premium=%s", payload["tenant_id"], payload["rule_id"], payload["user_id"], result.get("account_id"), result.get("telegram_user_id"), result.get("is_premium"))
        result["tenant_id"] = payload["tenant_id"]
        result["rule_id"] = payload["rule_id"]
        result["user_id"] = payload["user_id"]
        return _success_page(result, lang=lang)
    except ValueError as exc:
        if str(exc) == "Неверный 2FA-пароль. Проверьте пароль и попробуйте снова.":
            LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=ValueError | reason=password_invalid")
            return _password_page(token, str(data.get("phone") or ""), lang, error_text=str(exc))
        LOGGER.warning("REACTION_ONBOARDING_FAILED | error_type=ValueError")
        return _error_page(str(exc), lang)
    except Exception as exc:
        LOGGER.exception(
            "REACTION_ONBOARDING_FAILED | tenant_id=%s | rule_id=%s | user_id=%s | error_type=%s",
            payload.get("tenant_id") if "payload" in locals() else None,
            payload.get("rule_id") if "payload" in locals() else None,
            payload.get("user_id") if "payload" in locals() else None,
            exc.__class__.__name__,
        )

        recovered = _recover_success_result(request, payload) if "payload" in locals() else None
        if recovered:
            LOGGER.warning(
                "REACTION_ONBOARDING_SUCCESS_RECOVERED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | original_error_type=%s",
                payload.get("tenant_id"),
                payload.get("rule_id"),
                payload.get("user_id"),
                recovered.get("account_id"),
                exc.__class__.__name__,
            )
            return _success_page(recovered, recovered=True, lang=lang)

        return _error_page("Не удалось завершить вход. Попробуйте заново из Telegram-бота.", lang)


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def create_app() -> web.Application:
    app = web.Application()
    repo = create_repository()
    app["repo"] = repo
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
