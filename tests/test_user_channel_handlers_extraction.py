from pathlib import Path
import re


def _callbacks(source: str) -> set[str]:
    exact = set(re.findall(r'c\.data\s*==\s*"([^"]+)"', source))
    starts = set(re.findall(r'c\.data\.startswith\("([^"]+)"\)', source))
    return exact | starts


def test_user_channel_handlers_module_exists_and_symbols():
    p = Path('app/user_channel_handlers.py')
    assert p.exists()
    t = p.read_text(encoding='utf-8')
    assert 'class UserChannelHandlersContext' in t
    assert 'def register_user_channel_handlers' in t


def test_bot_registers_user_channel_handlers():
    t = Path('bot.py').read_text(encoding='utf-8')
    assert 'UserChannelHandlersContext(' in t
    assert 'register_user_channel_handlers(' in t


def test_moved_callbacks_in_module_not_in_bot():
    moved = [
        'user_sources_add',
        'user_targets_add',
        'user_channels_add',
        'user_sources_remove',
        'user_targets_remove',
        'user_channel_add_type:',
        'user_channel_add_entity:',
        'user_channel_remove_pick:',
        'user_channel_remove_cancel',
        'user_channel_remove_confirm:',
    ]
    bt = Path('bot.py').read_text(encoding='utf-8')
    mt = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')
    for marker in moved:
        assert marker in mt

    for exact in ['user_sources_add', 'user_channels_add', 'user_targets_add', 'user_channel_remove_cancel']:
        assert f'@dp.callback_query(lambda c: c.data == "{exact}")' not in bt
    for pref in ['user_channel_add_type:', 'user_channel_add_entity:', 'user_channel_remove_pick:', 'user_channel_remove_confirm:']:
        assert f'@dp.callback_query(lambda c: c.data and c.data.startswith("{pref}"))' not in bt


def test_user_states_present_for_channel_input_state():
    t = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')
    assert 'user_states' in t


def test_module_has_no_forbidden_markers_and_no_bot_import():
    t = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')
    assert 'import bot' not in t
    for marker in [
        'successful_payment', 'payment_confirm', 'payment_reject', 'create_invoice', 'product:',
        'user_status', 'user_account', 'user_plans', 'user_subscription',
        'rule_card:', 'trigger_now:', 'rescan_rule_', 'rollback:', 'start_from_number:',
        'video_intro_', 'intro_delete:', 'intro_view:', 'rule_repost_campaign',
    ]:
        assert marker not in t


def test_no_duplicate_callback_prefixes_across_selected_modules():
    files = [
        'bot.py',
        'app/user_menu_handlers.py',
        'app/user_status_handlers.py',
        'app/user_channel_handlers.py',
        'app/repost_campaign_handlers.py',
        'app/repost_campaign_schedule_handlers.py',
        'app/repost_campaign_report_handlers.py',
        'app/repost_campaign_scheduled_post_handlers.py',
        'app/repost_campaign_message_handlers.py',
    ]

    owners: dict[str, list[str]] = {}
    for file_path in files:
        source = Path(file_path).read_text(encoding='utf-8')
        for cb in _callbacks(source):
            owners.setdefault(cb, []).append(file_path)

    duplicates = {
        cb: file_owners
        for cb, file_owners in owners.items()
        if len(file_owners) > 1 and cb.startswith('user_')
    }
    assert not duplicates, f'Duplicate callback prefixes found: {duplicates}'
