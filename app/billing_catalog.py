from __future__ import annotations

USD_PRICES = {
    "basic": {1: 9, 3: 25, 6: 49, 12: 89},
    "pro": {1: 29, 3: 79, 6: 149, 12: 269},
}

DEFAULT_RATES = {"USD_TO_RUB": 95.0, "USD_TO_EUR": 0.9, "USD_TO_UAH": 40.0}
SYMBOLS = {"USD": "$", "RUB": "₽", "EUR": "€", "UAH": "₴"}


def _load_rates_from_repo(repo) -> dict[str, float]:
    rates = dict(DEFAULT_RATES)
    if not repo or not hasattr(repo, "get_billing_exchange_rates"):
        return rates
    try:
        saved = repo.get_billing_exchange_rates() or {}
    except Exception:
        return rates
    for key, fallback in DEFAULT_RATES.items():
        value = saved.get(key)
        try:
            rates[key] = float(value) if value is not None else float(fallback)
        except Exception:
            rates[key] = float(fallback)
    return rates


def _get_usd_price(tariff_code: str, period_months: int, repo=None) -> float:
    code = str(tariff_code).lower()
    period = int(period_months)
    fallback = float(USD_PRICES[code][period])
    if not repo or not hasattr(repo, "get_billing_usd_prices"):
        return fallback
    try:
        saved = repo.get_billing_usd_prices() or {}
    except Exception:
        return fallback
    try:
        value = saved.get(code, {}).get(period)
        return float(value) if value is not None else fallback
    except Exception:
        return fallback

def get_price(tariff_code: str, period_months: int, currency: str, repo=None) -> float:
    usd = _get_usd_price(tariff_code, period_months, repo=repo)
    c = str(currency).upper()
    if c == "USD":
        return usd
    rate_key = f"USD_TO_{c}"
    rates = _load_rates_from_repo(repo)
    rate = float(rates.get(rate_key, DEFAULT_RATES.get(rate_key, 1.0)))
    return round(usd * rate)

def format_price(tariff_code: str, period_months: int, currency: str, repo=None) -> str:
    amount = get_price(tariff_code, period_months, currency, repo=repo)
    c = str(currency).upper()
    return f"{int(amount)} {c}"
