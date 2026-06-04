from pathlib import Path
import ast
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


def test_channel_message_state_consumer_api_and_state_markers():
    t = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')
    assert 'async def handle_user_channel_state_message' in t
    assert '"awaiting_user_channel_id"' in t
    assert 'return False' in t
    assert 'return True' in t


def test_bot_registers_user_channel_handlers_and_delegates_message_state():
    t = Path('bot.py').read_text(encoding='utf-8')
    assert 'UserChannelHandlersContext(' in t
    assert 'register_user_channel_handlers(' in t
    assert 'handle_user_channel_state_message(message, user_channel_ctx)' in t


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


def test_bot_no_longer_owns_awaiting_user_channel_id_branch_body():
    bt = Path('bot.py').read_text(encoding='utf-8')
    mt = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')

    assert 'if action == "awaiting_user_channel_id":' not in bt
    assert 'if state.get("action") != "awaiting_user_channel_id":' in mt
    assert 'ctx.db.channel_exists, chat_id, None, channel_type' in mt
    assert 'ctx.db.add_channel_for_tenant, tenant_id, chat_id, None, channel_type, title, user_id_safe' in mt

    delegation_pos = bt.index('handle_user_channel_state_message(message, user_channel_ctx)')
    next_channel_branch_pos = bt.index('if action == "awaiting_user_channel_thread_id"', delegation_pos)
    delegated_gap = bt[delegation_pos:next_channel_branch_pos]
    assert 'channel_exists, chat_id, None, channel_type' not in delegated_gap
    assert 'add_channel_for_tenant, tenant_id, chat_id, None, channel_type' not in delegated_gap
    assert 'bot.get_chat(chat_id)' not in delegated_gap


def test_user_channel_callback_registration_still_present():
    t = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')
    assert 'register_user_channel_handlers' in t
    assert 'UserChannelHandlersContext' in t
    assert 'user_channel_add_type:' in t
    assert 'user_channel_add_entity:' in t
    assert 'user_channel_remove_confirm:' in t


def test_module_has_no_forbidden_markers_and_no_bot_import():
    t = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')
    assert 'import bot' not in t
    for marker in [
        'successful_payment', 'payment_confirm', 'payment_reject', 'create_invoice', 'product:',
        'user_status', 'user_account', 'user_plans', 'user_subscription',
        'rule_card:', 'trigger_now:', 'rescan_rule_', 'rollback:', 'start_from_number:',
        'video_intro_', 'intro_delete:', 'intro_view:', 'rule_repost_campaign',
        'waiting_vip_scheduled_post', 'repost_campaign_schedule_input',
    ]:
        assert marker not in t


def test_user_channel_context_has_no_unrelated_payment_scheduler_runtime_dependencies():
    source = Path('app/user_channel_handlers.py').read_text(encoding='utf-8')
    tree = ast.parse(source)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'UserChannelHandlersContext')
    fields = {stmt.target.id for stmt in cls.body if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)}

    assert not fields & {
        'payment_service',
        'subscription_service',
        'product_service',
        'scheduler_service',
        'runtime_service',
        'worker_policy',
        'transport_policy',
        'telethon_client',
    }


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
