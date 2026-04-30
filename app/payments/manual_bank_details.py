from __future__ import annotations

from typing import Any

from app.payments.payment_matrix import method_by_code


MANUAL_BANK_DETAILS: dict[str, dict[str, str]] = {
    "uah_abank": {
        "title": "🇺🇦 А-Банк",
        "card": "4323347388778133",
        "instruction": "Оплатите выбранный тариф переводом на банковскую карту 🇺🇦 А-Банк.",
    },
    "uah_oschad": {
        "title": "🇺🇦 Ощадбанк",
        "card": "4483820043174381",
        "instruction": "Оплатите выбранный тариф переводом на банковскую карту 🇺🇦 Ощадбанк.",
    },
    "uah_pumb": {
        "title": "🇺🇦 PUMB",
        "card": "5355280059027787",
        "instruction": "Оплатите выбранный тариф переводом на банковскую карту 🇺🇦 PUMB.",
    },
}


def get_manual_bank_details(method_code: str) -> dict[str, str] | None:
    code = str(method_code or "").strip().lower()
    if not code:
        return None
    details = MANUAL_BANK_DETAILS.get(code)
    if details:
        return details
    method: dict[str, Any] = method_by_code("UAH", code) or {}
    card = str(method.get("card") or "").strip()
    if not card:
        return None
    title = str(method.get("title") or code)
    return {
        "title": title,
        "card": card,
        "instruction": f"Оплатите выбранный тариф переводом на банковскую карту {title}.",
    }
