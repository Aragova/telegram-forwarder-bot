import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from aiogram import Dispatcher

from app.keyboards import get_main_menu, get_system_menu
from app.admin_handlers.menu import register_admin_menu_handlers
from app.admin_handlers.system import register_admin_system_handlers, BILLING_HOME_TEXT


def test_main_and_system_menu_exact_layout():
    assert [[b.text for b in row] for row in get_main_menu().keyboard] == [
        ["📈 Живой статус", "🔄 Правила"],
        ["📡 Каналы", "💳 Финансы"],
        ["⚠️ Диагностика", "⚙️ Система"],
    ]
    assert [[b.text for b in row] for row in get_system_menu().keyboard] == [
        ["▶️ Запустить пересылку", "⏸ Остановить пересылку"],
        ["⬅️ Назад в меню"],
    ]


def test_retired_queue_module_and_unsafe_calls_removed():
    root = Path(__file__).resolve().parents[1]
    assert not (root / "app/admin_handlers/queue.py").exists()
    for path in [root / "bot.py", *(root / "app").rglob("*.py")]:
        source = path.read_text(encoding="utf-8")
        for name in ("reset_all_deliveries", "reset_source_deliveries",
                     "reset_queue_for_source", "reset_all_queue",
                     "register_admin_queue_handlers", "get_reset_queue_menu",
                     "get_queue_menu", "build_reset_channels_keyboard",
                     "sources_inline_keyboard", "reset_source_inline"):
            assert name not in source, (path, name)


def test_old_reply_aliases_are_navigation_only():
    import ast
    root = Path(__file__).resolve().parents[1]
    tree = ast.parse((root / "bot.py").read_text(encoding="utf-8"))
    aliases = next(ast.literal_eval(n.value) for n in tree.body
                   if isinstance(n, ast.Assign)
                   and any(isinstance(x, ast.Name) and x.id == "ADMIN_REPLY_KEYBOARD_ALIASES" for x in n.targets))
    for label in ("📦 Очередь", "📋 Очередь", "📋 Общая очередь",
                  "🔄 Сброс", "🔄 Сбросить всё", "📊 Сброс по источнику"):
        assert aliases[label] == "📋 Меню"


def test_retired_callback_never_calls_database_or_nonadmin_navigation(monkeypatch):
    import app.admin_handlers.menu as menu
    monkeypatch.setattr(menu, "clear_folder_input", Mock())
    ctx = SimpleNamespace(
        is_admin_callback=AsyncMock(return_value=True), reset_user_state=Mock(),
        answer_callback_safe=AsyncMock(), send_message_safe=AsyncMock(),
        get_main_menu=get_main_menu, db=Mock(), run_db=AsyncMock(),
    )
    dp = Dispatcher()
    register_admin_menu_handlers(dp, ctx)
    handler = next(h.callback for h in dp.callback_query.handlers
                   if h.callback.__name__ == "handle_retired_queue_callback")
    for data in ("reset_back", "reset_source:0", "reset_source:-1", "reset_source:invalid"):
        asyncio.run(handler(SimpleNamespace(data=data, from_user=SimpleNamespace(id=1),
            message=SimpleNamespace(chat=SimpleNamespace(id=1, type="private")))))
    assert ctx.send_message_safe.await_count == 4
    ctx.run_db.assert_not_awaited()
    assert not ctx.db.mock_calls
    ctx.is_admin_callback.return_value = False
    asyncio.run(handler(SimpleNamespace(data="reset_source:0", from_user=SimpleNamespace(id=2), message=None)))
    assert ctx.send_message_safe.await_count == 4
    assert ctx.reset_user_state.call_count == 4


def test_finance_back_restores_main_menu():
    ctx = SimpleNamespace(
        is_admin_callback=AsyncMock(return_value=True), is_admin=AsyncMock(return_value=True),
        reset_user_state=Mock(), answer_callback_safe_once=AsyncMock(),
        edit_message_text_safe=AsyncMock(), send_message_safe=AsyncMock(),
        get_main_menu=get_main_menu,
    )
    dp = Dispatcher()
    register_admin_system_handlers(dp, ctx)
    home = next(h.callback for h in dp.message.handlers if h.callback.__name__ == "handle_billing_home")
    back = next(h.callback for h in dp.callback_query.handlers if h.callback.__name__ == "handle_billing_home_back")
    message = SimpleNamespace(from_user=SimpleNamespace(id=1), chat=SimpleNamespace(id=1))
    asyncio.run(home(message))
    assert ctx.send_message_safe.call_args.kwargs["text"] == BILLING_HOME_TEXT
    assert ctx.send_message_safe.call_args.kwargs["parse_mode"] == "HTML"
    asyncio.run(back(SimpleNamespace(from_user=message.from_user, message=message)))
    assert ctx.send_message_safe.call_args.kwargs["reply_markup"] == get_main_menu()
    ctx.is_admin_callback.return_value = False
    asyncio.run(back(SimpleNamespace(from_user=message.from_user, message=message)))
    assert ctx.send_message_safe.await_count == 2


def test_finance_filter_accepts_new_and_legacy_buttons_but_not_nontext():
    dp = Dispatcher()
    register_admin_system_handlers(dp, SimpleNamespace())
    handler = next(h for h in dp.message.handlers
                   if h.callback.__name__ == "handle_billing_home")
    predicate = handler.filters[0].callback
    for value in (None, "", " ", "📦 Очередь", "⚙️ Система"):
        assert not predicate(SimpleNamespace(text=value))
    for value in ("💳 Финансы", "💳 Биллинг и тарифы", " 💳 Финансы ",
                  "💳\ufe0f Финансы"):
        assert predicate(SimpleNamespace(text=value))


def test_finance_navigation_preempts_scheduled_material_handler():
    import ast
    from app.admin_handlers.system import show_billing_home

    source = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    names = {"normalize_reply_text", "ADMIN_REPLY_KEYBOARD_ALIASES", "handle_admin_reply_keyboard_router"}
    nodes = [node for node in tree.body if (
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
        or isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in names for target in node.targets)
    )]
    router = next(node for node in nodes if isinstance(node, ast.AsyncFunctionDef))
    assert router.lineno < source[:source.index("register_repost_campaign_message_handlers(dp,")].count("\n") + 1
    predicate = router.decorator_list[0].args[0]
    router.decorator_list = []
    states = {}
    reset = Mock(side_effect=lambda uid: states.pop(uid, None))
    clear = Mock()
    send = AsyncMock()
    env = {
        "Message": object, "_is_admin_user": lambda uid: uid == 1,
        "is_reaction_account_reactions_input_state": lambda *_: False,
        "user_states": states, "logger": Mock(), "reset_user_state": reset,
        "clear_folder_input": clear, "send_message_safe": send,
        "show_billing_home": show_billing_home,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "bot.py", "exec"), env)
    matches = eval(compile(ast.Expression(predicate), "bot.py", "eval"), env)
    for label in ("💳 Финансы", "💳 Биллинг и тарифы", " 💳\ufe0f Финансы "):
        states[1] = {"state": "waiting_vip_scheduled_post_material"}
        message = SimpleNamespace(text=label, from_user=SimpleNamespace(id=1),
                                  chat=SimpleNamespace(id=1, type="private"))
        assert matches(message)
        asyncio.run(env["handle_admin_reply_keyboard_router"](message))
        assert 1 not in states
        clear.assert_called_with(1)
        assert send.call_args.kwargs["text"] == BILLING_HOME_TEXT
        assert send.call_args.kwargs["parse_mode"] == "HTML"
    assert send.await_count == 3
    assert not matches(SimpleNamespace(text="💳 Финансы", from_user=SimpleNamespace(id=2),
                                       chat=SimpleNamespace(id=2, type="private")))
