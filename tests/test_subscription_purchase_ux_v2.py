from app import user_ui


def _texts(keyboard):
    return [b.text for r in keyboard.inline_keyboard for b in r]


def test_state_a_currency_row_single_and_selected_marker():
    prices = {1: '9 USD', 3: '25 USD', 6: '49 USD', 12: '89 USD'}
    kb = user_ui.build_user_tariff_period_select_keyboard(tariff_code='basic', currency='USD', prices=prices)
    assert len(kb.inline_keyboard[0]) == 4
    assert kb.inline_keyboard[0][0].text.startswith('🟢')


def test_state_a_shows_periods_with_prices_and_no_methods():
    prices = {1: '9 USD', 3: '25 USD', 6: '49 USD', 12: '89 USD'}
    text = user_ui.build_user_tariff_period_select_text('basic')
    kb = user_ui.build_user_tariff_period_select_keyboard(tariff_code='basic', currency='USD', prices=prices)
    labels = _texts(kb)
    assert 'Выберите срок подписки' in text
    assert any('— 9 USD' in t for t in labels)
    assert not any('PayPal' in t or 'Crypto' in t or 'Stars' in t for t in labels)


def test_state_b_shows_methods_and_hides_period_buttons():
    prices = {1: '9 USD', 3: '25 USD', 6: '49 USD', 12: '89 USD'}
    text = user_ui.build_user_tariff_payment_select_text('basic', 'USD', 1, prices)
    kb = user_ui.build_user_tariff_payment_select_keyboard(tariff_code='basic', currency='USD', pay_buttons=[('🅿️ PayPal через Lava.top','a1'), ('₿ Crypto','a2')])
    labels = _texts(kb)
    assert 'Цена: 9 USD' in text
    assert 'Срок действия: 1 месяц' in text
    assert any('PayPal' in t for t in labels)
    assert not any('🎉 1 месяц' in t for t in labels)


def test_currency_switch_in_state_b_keeps_period_and_changes_amount():
    usd_prices = {1: '9 USD', 3: '25 USD', 6: '49 USD', 12: '89 USD'}
    rub_prices = {1: '855 RUB', 3: '2375 RUB', 6: '4655 RUB', 12: '8455 RUB'}
    usd_text = user_ui.build_user_tariff_payment_select_text('basic', 'USD', 1, usd_prices)
    rub_text = user_ui.build_user_tariff_payment_select_text('basic', 'RUB', 1, rub_prices)
    assert 'Срок действия: 1 месяц' in usd_text and 'Срок действия: 1 месяц' in rub_text
    assert 'Цена: 9 USD' in usd_text
    assert 'Цена: 855 RUB' in rub_text
