from pathlib import Path

import pytest

from app import user_ui
from app.payments.payment_matrix import method_by_code, methods_for_currency


EXTERNAL_LAVA_EUR_LINK = (
    "https://app.lava.top/products/"
    "6846f916-88b7-4266-93d9-53f9d81767f8/"
    "16731707-ffba-466a-80a5-2d4002f33c64?currency=EUR"
)


def test_lava_external_is_only_offered_for_eur():
    method = method_by_code("EUR", "lava_external_eur")
    assert method is not None
    assert method["title"] == "💳 LAVA.TOP 🇪🇺"
    assert method["checkout_url"] == EXTERNAL_LAVA_EUR_LINK
    for currency in ("USD", "RUB", "UAH"):
        assert method_by_code(currency, "lava_external_eur") is None


@pytest.mark.parametrize("tariff", ["basic", "pro"])
@pytest.mark.parametrize("period", [1, 3, 6, 12])
def test_external_eur_checkout_screen_for_both_tariffs_and_all_periods(tariff, period):
    method = method_by_code("EUR", "lava_external_eur")
    text = user_ui.build_lava_external_eur_text(
        tariff_code=tariff, period_months=period, checkout_url=method["checkout_url"],
    )
    keyboard = user_ui.build_lava_external_eur_keyboard(
        tariff_code=tariff, period_months=period, checkout_url=method["checkout_url"],
    )
    assert tariff.upper() in text
    assert EXTERNAL_LAVA_EUR_LINK in text
    assert "To gain access to the premium channel" in text
    assert "price using this payment method may differ" in text
    assert "не активирует подписку автоматически" in text
    assert keyboard.inline_keyboard[0][0].url == EXTERNAL_LAVA_EUR_LINK
    assert keyboard.inline_keyboard[1][0].callback_data == "user_support"
    assert keyboard.inline_keyboard[2][0].callback_data == f"user_subscription_methods:{tariff}:EUR:{period}"


def test_external_method_is_in_standard_eur_method_list():
    methods = methods_for_currency("EUR")
    assert [method["code"] for method in methods].count("lava_external_eur") == 1


def test_external_checkout_does_not_call_invoice_router():
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    handler_start = source.index('if method_code == "lava_external_eur"')
    router_start = source.index('await router.start_payment(', handler_start)
    method_block = source[handler_start:router_start]
    assert "build_lava_external_eur_keyboard" in method_block
    assert "return" in method_block
