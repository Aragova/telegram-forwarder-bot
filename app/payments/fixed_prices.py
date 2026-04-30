from __future__ import annotations

from typing import Any

from app.billing_catalog import USD_PRICES

FIXED_STARS_PRICES: dict[str, dict[int, int]] = {
    "basic": {1: 900, 3: 2500, 6: 4900, 12: 8900},
    "pro": {1: 2900, 3: 7900, 6: 14900, 12: 26900},
}

FIXED_CRYPTO_PRICES: dict[str, dict[int, dict[str, str]]] = {
    "basic": {
        1: {"amount": "9", "display": "$9"},
        3: {"amount": "25", "display": "$25"},
        6: {"amount": "49", "display": "$49"},
        12: {"amount": "89", "display": "$89"},
    },
    "pro": {
        1: {"amount": "29", "display": "$29"},
        3: {"amount": "79", "display": "$79"},
        6: {"amount": "149", "display": "$149"},
        12: {"amount": "269", "display": "$269"},
    },
}


def get_stars_price(tariff_code: str, period_months: int, repo: Any | None = None) -> int | None:
    code = str(tariff_code).lower()
    period = int(period_months)
    if repo and hasattr(repo, "get_billing_fixed_prices"):
        saved = repo.get_billing_fixed_prices("stars") or {}
        raw = (saved.get(code) or {}).get(period)
        if isinstance(raw, dict) and raw.get("amount") is not None:
            try:
                return int(raw.get("amount"))
            except Exception:
                pass
    return FIXED_STARS_PRICES.get(code, {}).get(period)


def format_stars_price(tariff_code: str, period_months: int, repo: Any | None = None) -> str:
    value = get_stars_price(tariff_code, period_months, repo=repo)
    if value is not None:
        return f"{int(value)} Stars"
    usd = int(USD_PRICES[str(tariff_code).lower()][int(period_months)])
    return f"{usd * 100} Stars"


def get_crypto_price(tariff_code: str, period_months: int, repo: Any | None = None) -> dict[str, str]:
    code = str(tariff_code).lower()
    period = int(period_months)
    if repo and hasattr(repo, "get_billing_fixed_prices"):
        saved = repo.get_billing_fixed_prices("crypto") or {}
        raw = (saved.get(code) or {}).get(period)
        if isinstance(raw, dict) and raw.get("amount") is not None:
            amount = str(raw.get("amount"))
            display = str(raw.get("display") or f"${amount}")
            return {"amount": amount, "display": display}
    value = FIXED_CRYPTO_PRICES.get(code, {}).get(period)
    if value:
        return value
    usd = int(USD_PRICES[str(tariff_code).lower()][int(period_months)])
    return {"amount": str(usd), "display": f"${usd}"}


def format_crypto_price(tariff_code: str, period_months: int, repo: Any | None = None) -> str:
    return str(get_crypto_price(tariff_code, period_months, repo=repo).get("display") or "—")
