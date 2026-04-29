from __future__ import annotations

USD_PRICES = {
    "basic": {1: 9, 3: 25, 6: 49, 12: 89},
    "pro": {1: 29, 3: 79, 6: 149, 12: 269},
}

DEFAULT_RATES = {"USD_TO_RUB": 95.0, "USD_TO_EUR": 0.9, "USD_TO_UAH": 40.0}
SYMBOLS = {"USD": "$", "RUB": "₽", "EUR": "€", "UAH": "₴"}


def get_price(tariff_code: str, period_months: int, currency: str) -> float:
    usd = float(USD_PRICES[str(tariff_code).lower()][int(period_months)])
    c = str(currency).upper()
    if c == "USD":
        return usd
    rate_key = f"USD_TO_{c}"
    rate = float(DEFAULT_RATES.get(rate_key, 1.0))
    return round(usd * rate)

def format_price(tariff_code: str, period_months: int, currency: str) -> str:
    amount = get_price(tariff_code, period_months, currency)
    c = str(currency).upper()
    return f"{int(amount)} {c}"
