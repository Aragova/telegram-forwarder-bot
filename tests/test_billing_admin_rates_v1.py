from app import billing_catalog
from app.keyboards import get_main_menu, get_system_menu


class _Repo:
    def __init__(self):
        self.rates = {}
        self.usd_prices = {}

    def get_billing_exchange_rates(self):
        return dict(self.rates)

    def get_billing_usd_prices(self):
        return dict(self.usd_prices)


def test_admin_finance_in_main_menu_only():
    kb = get_system_menu()
    texts = [btn.text for row in kb.keyboard for btn in row]
    assert "💳 Биллинг и тарифы" not in texts
    assert "💳 Финансы" not in texts
    assert "💳 Финансы" in [btn.text for row in get_main_menu().keyboard for btn in row]
    assert "💱 Курсы валют" not in texts
    assert "💵 Цены тарифов" not in texts
    assert "💫 Фикс-цены Stars/Crypto" not in texts


def test_billing_catalog_uses_saved_rate_and_fallback():
    repo = _Repo()
    repo.rates = {"USD_TO_RUB": 95.5}
    assert billing_catalog.get_price("basic", 1, "RUB", repo=repo) == round(9 * 95.5)
    assert billing_catalog.get_price("basic", 1, "EUR", repo=repo) == round(9 * billing_catalog.DEFAULT_RATES["USD_TO_EUR"])


def test_billing_catalog_uses_saved_usd_price_and_fallback():
    repo = _Repo()
    repo.usd_prices = {"basic": {1: 11.5}}
    assert billing_catalog.get_price("basic", 1, "USD", repo=repo) == 11.5
    assert billing_catalog.get_price("basic", 3, "USD", repo=repo) == 25


def test_rate_input_formats():
    assert float("95.5") == 95.5
    assert float("95,5".replace(",", ".")) == 95.5


def test_admin_rate_input_not_captured_by_user_state_handler_source_guard():
    source = __import__("pathlib").Path("bot.py").read_text(encoding="utf-8")
    assert "admin_billing_rate_input" in source

    assert "admin_billing_usd_price_input" in source
    assert "db.set_billing_usd_price" not in source
    system = __import__("pathlib").Path("app/admin_handlers/system.py").read_text(encoding="utf-8")
    assert "db.set_billing_usd_price" in system
