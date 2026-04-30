from app.payments.crypto_wallets import get_crypto_wallet, list_crypto_wallets
from app.payments.fixed_prices import format_crypto_price, format_stars_price, get_crypto_price
from app.payments.payment_matrix import PAYMENT_MATRIX


def test_crypto_and_stars_are_methods_inside_currencies_not_currencies():
    assert "CRYPTO" not in PAYMENT_MATRIX
    assert "STARS" not in PAYMENT_MATRIX
    for currency in ("RUB", "USD", "EUR", "UAH"):
        codes = {m["code"] for m in PAYMENT_MATRIX[currency]}
        assert "crypto" in codes
        assert "stars" in codes


def test_crypto_wallet_registry_values():
    assert get_crypto_wallet("btc")["wallet_address"] == "1MsgkYc918tJMkbnAxXfCJRqpPZrVL3GYN"
    assert get_crypto_wallet("usdt_trc20")["wallet_address"] == "TDfU79B6RwVL7DpKHZu1GjSWYSKY39JDgf"
    assert get_crypto_wallet("eth")["wallet_address"] == "0xa25ee4e00c2b578afedfccb5e2c90a996ee21cdd"
    assert get_crypto_wallet("trx")["wallet_address"] == "TDfU79B6RwVL7DpKHZu1GjSWYSKY39JDgf"
    assert get_crypto_wallet("ton")["wallet_address"] == "UQDI5eY8YaVLgWVJjX-iMoXBroNnmSL2S4WsYZopK9cM0CIF"
    assert len(list_crypto_wallets()) >= 5


def test_fixed_crypto_price_independent_from_currency():
    assert format_crypto_price("basic", 1) == "$9"
    assert format_crypto_price("basic", 1) == "$9"
    price = get_crypto_price("pro", 12)
    assert price["display"] == "$269"


def test_stars_price_format():
    assert "Stars" in format_stars_price("basic", 1)


class _Repo:
    def __init__(self):
        self.fixed = {}
    def get_billing_fixed_prices(self, kind):
        return self.fixed.get(kind, {})


def test_repo_fixed_prices_override_and_fallback():
    repo = _Repo()
    repo.fixed = {"stars": {"basic": {1: {"amount": 950}}}, "crypto": {"basic": {1: {"amount": "9.5", "display": "$9.5"}}}}
    assert format_stars_price("basic", 1, repo=repo) == "950 Stars"
    assert format_stars_price("basic", 3, repo=repo) == "2500 Stars"
    assert format_crypto_price("basic", 1, repo=repo) == "$9.5"
    assert format_crypto_price("basic", 3, repo=repo) == "$25"
