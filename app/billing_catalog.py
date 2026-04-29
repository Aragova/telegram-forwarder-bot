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

def get_price(tariff_code: str, period_months: int, currency: str, repo=None) -> float:
    usd = float(USD_PRICES[str(tariff_code).lower()][int(period_months)])
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
