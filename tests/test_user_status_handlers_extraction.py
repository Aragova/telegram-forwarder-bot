from pathlib import Path
import re


def test_user_status_handlers_module_exists_and_symbols():
    p = Path('app/user_status_handlers.py')
    assert p.exists()
    t = p.read_text(encoding='utf-8')
    assert 'class UserStatusHandlersContext' in t
    assert 'def register_user_status_handlers' in t
    assert 'lambda c: c.data == "user_status"' in t
    for marker in ['subscription_grace_warning_shown', 'limit_service', 'usage_service', 'get_tenant_queue_stats']:
        assert marker in t


def test_bot_registers_user_status_handlers_and_removes_inline_handler():
    t = Path('bot.py').read_text(encoding='utf-8')
    assert 'register_user_status_handlers(' in t
    assert 'UserStatusHandlersContext(' in t
    assert '@dp.callback_query(lambda c: c.data == "user_status")' not in t


def test_user_status_module_has_no_forbidden_markers_and_no_bot_import():
    t = Path('app/user_status_handlers.py').read_text(encoding='utf-8')
    assert 'import bot' not in t
    for marker in [
        'successful_payment', 'payment_confirm', 'payment_reject', 'create_invoice', 'product:',
        'user_channel_add', 'user_channel_remove', 'user_sources_add', 'user_targets_add',
        'rule_card:', 'trigger_now:', 'rescan_rule_', 'rollback:', 'start_from_number:',
        'video_intro_', 'intro_delete:', 'intro_view:', 'rule_repost_campaign',
    ]:
        assert marker not in t


def test_no_duplicate_callback_prefixes_across_selected_modules():
    files = [
        'bot.py',
        'app/user_menu_handlers.py',
        'app/user_status_handlers.py',
        'app/repost_campaign_handlers.py',
        'app/repost_campaign_schedule_handlers.py',
        'app/repost_campaign_report_handlers.py',
        'app/repost_campaign_scheduled_post_handlers.py',
        'app/repost_campaign_message_handlers.py',
    ]
    pattern = re.compile(r'@dp\.callback_query\(lambda c: c\.data\.startswith\("([^"]+)"\)\)')
    prefixes: dict[str, list[str]] = {}
    for file_path in files:
        source = Path(file_path).read_text(encoding='utf-8')
        for prefix in pattern.findall(source):
            prefixes.setdefault(prefix, []).append(file_path)
    duplicates = {prefix: owners for prefix, owners in prefixes.items() if len(owners) > 1}
    assert not duplicates, f'Duplicate callback prefixes found: {duplicates}'


def test_live_status_handles_unavailable_queue_stats():
    import asyncio
    from types import SimpleNamespace
    from app.user_status_handlers import register_user_status_handlers

    handlers = []
    edits = []
    class Dispatcher:
        def callback_query(self, predicate):
            def register(fn):
                handlers.append(fn)
                return fn
            return register

    async def run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def answer(*args, **kwargs):
        pass

    async def edit(**kwargs):
        edits.append(kwargs)

    def unavailable(tenant):
        raise TimeoutError("query timed out")

    ctx = SimpleNamespace(
        db=SimpleNamespace(get_tenant_queue_stats=unavailable),
        _is_admin_user=lambda user_id: False,
        ensure_user_tenant=lambda user_id: 1, run_db=run_db,
        subscription_service=SimpleNamespace(get_active_subscription=lambda tenant: {}),
        usage_service=SimpleNamespace(get_today_usage=lambda tenant: {}),
        limit_service=SimpleNamespace(can_create_rule=lambda tenant: (True, ""),
            can_enqueue_job=lambda tenant: (True, ""), can_process_video=lambda tenant: (True, "")),
        is_subscription_blocked_status=lambda status: False, USER_TZ=None,
        answer_callback_safe_once=answer, edit_message_text_safe=edit,
    )
    register_user_status_handlers(Dispatcher(), ctx)
    asyncio.run(handlers[0](SimpleNamespace(from_user=SimpleNamespace(id=1), message=object())))
    assert "В очереди: —" in edits[0]["text"]
    assert "Статистика временно недоступна" in edits[0]["text"]
    assert "query timed out" not in edits[0]["text"]
