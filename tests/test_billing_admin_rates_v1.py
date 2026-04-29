from app import billing_catalog
from app.keyboards import get_system_menu


class _Repo:
    def __init__(self):
        self.rates = {}

    def get_billing_exchange_rates(self):
        return dict(self.rates)


def test_admin_system_menu_contains_rates_button():
    kb = get_system_menu()
    texts = [btn.text for row in kb.keyboard for btn in row]
    assert "💱 Курсы валют" in texts


def test_billing_catalog_uses_saved_rate_and_fallback():
    repo = _Repo()
    repo.rates = {"USD_TO_RUB": 95.5}
    assert billing_catalog.get_price("basic", 1, "RUB", repo=repo) == round(9 * 95.5)
    assert billing_catalog.get_price("basic", 1, "EUR", repo=repo) == round(9 * billing_catalog.DEFAULT_RATES["USD_TO_EUR"])


def test_rate_input_formats():
    assert float("95.5") == 95.5
    assert float("95,5".replace(",", ".")) == 95.5
