from pathlib import Path


def test_user_menu_handlers_module_exists_and_symbols():
    p = Path('app/user_menu_handlers.py')
    assert p.exists()
    t = p.read_text(encoding='utf-8')
    assert 'def register_user_menu_handlers' in t
    assert 'class UserMenuHandlersContext' in t


def test_bot_registers_user_menu_handlers():
    t = Path('bot.py').read_text(encoding='utf-8')
    assert 'register_user_menu_handlers(dp, user_menu_ctx)' in t


def test_moved_prefixes_not_in_bot_and_in_module():
    moved = ['user_main', 'user_channels', 'user_sources', 'user_targets', 'user_account', 'user_plans']
    bt = Path('bot.py').read_text(encoding='utf-8')
    mt = Path('app/user_menu_handlers.py').read_text(encoding='utf-8')
    for pref in moved:
        assert pref in mt
        assert f'c.data == "{pref}"' not in bt


def test_no_bot_import_and_no_high_risk_markers():
    t = Path('app/user_menu_handlers.py').read_text(encoding='utf-8')
    assert 'import bot' not in t
    for marker in [
        'successful_payment', 'payment_confirm', 'payment_reject', 'create_invoice',
        'payment_intent', 'activate', 'product:', 'rule_card:', 'trigger_now:',
        'rescan_rule_', 'rollback:', 'start_from_number:', 'video_intro_',
        'intro_delete:', 'intro_view:', 'rule_repost_campaign',
        'write_billing_event', 'subscription_grace_warning_shown', 'limit_service',
        'usage_service', 'get_rule_card_snapshot',
    ]:
        assert marker not in t


def test_user_status_not_in_user_menu_module():
    mt = Path('app/user_menu_handlers.py').read_text(encoding='utf-8')
    assert "lambda c: c.data == \"user_status\"" not in mt
