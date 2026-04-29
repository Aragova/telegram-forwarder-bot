from pathlib import Path


def test_billing_support_buttons_route_to_user_support():
    source = Path('app/user_handlers/payments.py').read_text(encoding='utf-8')
    assert 'Поддержка", callback_data="user_main"' not in source
    assert source.count('Поддержка", callback_data="user_support"') >= 3
