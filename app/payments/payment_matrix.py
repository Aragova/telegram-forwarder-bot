from __future__ import annotations

from typing import Any
from app.config import settings


def _stars_enabled() -> bool:
    return bool(settings.payment_enabled and settings.telegram_stars_enabled)

PAYMENT_MATRIX: dict[str, list[dict[str, Any]]] = {
    "RUB": [
        {"code": "rub_card_sbp", "title": "💳 Карта / СБП РФ", "provider": "manual_bank_card"},
        {"code": "rub_lava", "title": "Lava.top RUB", "provider": "lava_top"},
        {"code": "stars", "title": "⭐ Telegram Stars", "provider": "telegram_stars", "enabled": _stars_enabled()},
        {"code": "crypto", "title": "₿ Crypto", "provider": "crypto_manual"},
    ],
    "USD": [
        {"code": "lava_card_usd", "title": "💳 Карта USD", "provider": "lava_top", "payment_provider": "UNLIMINT"},
        {"code": "lava_paypal_usd", "title": "🅿️ PayPal через Lava.top", "provider": "lava_top", "payment_provider": "PAYPAL"},
        {"code": "paypal_manual_usd", "title": "🅿️ PayPal вручную", "provider": "manual_paypal", "email": "hurremae@gmail.com"},
        {"code": "tribute_usd", "title": "💎 Tribute", "provider": "tribute", "enabled": False},
        {"code": "stars", "title": "⭐ Telegram Stars", "provider": "telegram_stars", "enabled": _stars_enabled()},
        {"code": "crypto", "title": "₿ Crypto", "provider": "crypto_manual"},
    ],
    "EUR": [
        {"code": "lava_card_eur", "title": "💳 Карта EUR", "provider": "lava_top", "payment_provider": "UNLIMINT"},
        {"code": "lava_paypal_eur", "title": "🅿️ PayPal через Lava.top", "provider": "lava_top", "payment_provider": "PAYPAL"},
        {"code": "paypal_manual_eur", "title": "🅿️ PayPal вручную", "provider": "manual_paypal", "email": "hurremae@gmail.com"},
        {"code": "tribute_eur", "title": "💎 Tribute", "provider": "tribute", "enabled": False},
        {"code": "stars", "title": "⭐ Telegram Stars", "provider": "telegram_stars", "enabled": _stars_enabled()},
        {"code": "crypto", "title": "₿ Crypto", "provider": "crypto_manual"},
    ],
    "UAH": [
        {"code": "uah_abank", "title": "🇺🇦 А-Банк", "provider": "manual_bank_card", "card": "4323347388778133"},
        {"code": "uah_oschad", "title": "🇺🇦 Ощадбанк", "provider": "manual_bank_card", "card": "4483820043174381"},
        {"code": "uah_pumb", "title": "🇺🇦 PUMB", "provider": "manual_bank_card", "card": "5355280059027787"},
        {"code": "stars", "title": "⭐ Telegram Stars", "provider": "telegram_stars", "enabled": _stars_enabled()},
        {"code": "crypto", "title": "₿ Crypto", "provider": "crypto_manual"},
    ],
}

def methods_for_currency(currency: str) -> list[dict[str, Any]]:
    items = PAYMENT_MATRIX.get(str(currency or "USD").upper(), PAYMENT_MATRIX["USD"])
    result: list[dict[str, Any]] = []
    for method in items:
        if str(method.get("code")) == "stars":
            result.append({**method, "enabled": _stars_enabled()})
        else:
            result.append(dict(method))
    return result

def method_by_code(currency: str, method_code: str) -> dict[str, Any] | None:
    code = str(method_code or "")
    for method in methods_for_currency(currency):
        if str(method.get("code")) == code:
            return method
    return None
