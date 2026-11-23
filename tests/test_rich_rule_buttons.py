"""Telegram Bot API 10.3 in-message buttons with instant classic fallback."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

from app.rich_rule_buttons import (
    _button_rows,
    _rule_id_for_callback,
    rich_catalog_payload,
    rich_edit_payload,
    rich_rule_list_enabled,
    try_edit_rich_catalog,
)
from app.rule_folders_ui import folder_fill_view, rule_catalog_view


@dataclass
class Rule:
    id: int
    target_title: str
    source_title: str = "Источник"
    is_active: bool = True
    target_id: str = "-10002"
    source_id: str = "-10001"
    source_thread_id: int | None = None
    target_thread_id: int | None = None


def _callbacks(markup):
    return [
        btn.callback_data
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data
    ]


def test_feature_flag_can_disable_without_deleting_classic_ui(monkeypatch):
    monkeypatch.delenv("VIMI_RICH_RULE_LIST", raising=False)
    assert rich_rule_list_enabled()
    for disabled in ["0", "false", "Off", "NO"]:
        monkeypatch.setenv("VIMI_RICH_RULE_LIST", disabled)
        assert not rich_rule_list_enabled()
    monkeypatch.setenv("VIMI_RICH_RULE_LIST", "1")
    assert rich_rule_list_enabled()


def test_real_rich_buttons_replace_number_buttons_and_preserve_callbacks():
    rules = [Rule(i, f"Цель {i}") for i in range(1, 11)]
    text, keyboard = rule_catalog_view(
        rules, folder_id=-1, folder_name="", page=0,
    )
    built = rich_catalog_payload(
        text, keyboard, rules, assigned={1: {"id": 9, "name": "ПРИВАТЫ"}},
    )
    assert built is not None
    html, footer = built
    assert html.count('<tg-button type="callback_data"') == 10
    assert html.count("<tg-button-row") == 5
    assert "🟢📁 #1 Цель 1" in html
    assert "🟢 #2 Цель 2" in html
    assert "📁 #2 Цель 2" not in html
    assert "rf:open:1:-1:0" in html
    assert "rf:open:10:-1:0" in html
    assert "→" not in html
    assert not any(cb.startswith("rf:open:") for cb in _callbacks(footer))
    assert "rf:list:-1:1" not in _callbacks(footer)
    assert "rf:search" in _callbacks(footer)
    assert "rf:batch:-1:0" in _callbacks(footer)
    assert footer.inline_keyboard[-1][0].callback_data == "rf:root"
    # The original markup stays intact for rollback.
    assert len([cb for cb in _callbacks(keyboard) if cb.startswith("rf:open:")]) == 10


def test_long_labels_get_full_width_and_html_is_escaped():
    rules = [
        Rule(4, "Light Dante"),
        Rule(14, "Чрезвычайно длинное название приватного получателя <VIP> & Friends"),
        Rule(19, "SHALUN REZERV"),
    ]
    text, kb = rule_catalog_view(rules, folder_id=-1, folder_name="", page=0)
    html, _ = rich_catalog_payload(text, kb, rules)
    assert html.count("<tg-button-row") == 3
    assert "Чрезвычайно длинное название приватного получателя &lt;VIP&gt; &amp; Friends" in html
    assert "<VIP>" not in html
    assert "rf:open:14:-1:0" in html


def test_page_and_search_callbacks_are_reused_verbatim():
    rules = [Rule(i, f"Title {i}") for i in range(20)]
    text, kb = rule_catalog_view(
        rules, folder_id=-1, folder_name="", page=1, search_term="title",
    )
    html, footer = rich_catalog_payload(text, kb, rules)
    assert "rf:open_search:10:1" in html
    assert "rf:open_search:19:1" in html
    assert "rf:results:0" in _callbacks(footer)
    assert "rf:results:1" not in _callbacks(footer)


def test_bulk_picker_and_folder_import_remain_clickable():
    rules = [Rule(i, f"Channel {i}") for i in range(1, 11)]
    text, kb = rule_catalog_view(
        rules, folder_id=9, folder_name="ПРИВАТЫ", page=0,
        selected={1, 2, 5},
    )
    html, footer = rich_catalog_payload(
        text, kb, rules,
        assigned={1: {"id": 9}, 2: {"id": 9}},
        selected={1, 2, 5},
    )
    assert "✅ 🟢📁 #1 Channel 1" in html
    assert "☐ 🟢 #3 Channel 3" in html
    assert "rf:pick:9:0:1" in html
    assert "rf:unfile:9:0" in _callbacks(footer)
    assert "rf:bulk_to:9" in _callbacks(footer)

    text2, kb2 = folder_fill_view(
        rules, folder_id=9, folder_name="ПРИВАТЫ",
        page=0, selected={4, 5}, assignments={5: {"id": 12, "name": "Другая"}},
    )
    html2, footer2 = rich_catalog_payload(
        text2, kb2, rules,
        selected={4, 5}, assigned={5: {"id": 12, "name": "Другая"}},
    )
    assert "rf:fill_pick:9:0:4" in html2
    assert "✅ 🟢 #4 Channel 4" in html2
    assert "✅ 🟢📁 #5 Channel 5" in html2
    assert "rf:fill_apply:9" in _callbacks(footer2)
    assert "rf:fill_page_all:9:0" in _callbacks(footer2)


def test_invalid_or_missing_rule_does_not_create_wrong_rich_button():
    rules = [Rule(10, "A")]
    text, kb = rule_catalog_view(rules, folder_id=-1, folder_name="", page=0)
    assert rich_catalog_payload(text, kb, rules[:0]) is None
    assert _rule_id_for_callback("rf:open:10:-1:0") == 10
    assert _rule_id_for_callback("rf:open_search:10:1") == 10
    assert _rule_id_for_callback("rf:pick:0:1:10") == 10
    assert _rule_id_for_callback("rf:fill_pick:0:1:10") == 10
    assert _rule_id_for_callback("rf:root") is None


def test_rich_payload_exact_bot_api_parameters():
    rules = [Rule(4, "Light Dante")]
    text, kb = rule_catalog_view(rules, folder_id=-1, folder_name="", page=0)
    html, footer = rich_catalog_payload(text, kb, rules)
    payload = rich_edit_payload(
        chat_id=999, message_id=42, html=html, footer=footer,
    )
    assert set(payload) == {"chat_id", "message_id", "rich_message", "reply_markup"}
    assert payload["rich_message"] == {"html": html}
    assert payload["message_id"] == 42
    assert isinstance(payload["reply_markup"]["inline_keyboard"], list)


def test_bot_api_rich_success_and_old_server_fallback():
    class StubClient:
        def __init__(self, response):
            self.response = response
            self.last_request = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def post(self, url, json):
            self.last_request = (url, json)
            return SimpleNamespace(status_code=400, json=lambda: self.response)

    bot = SimpleNamespace(
        token="example_token",
        session=SimpleNamespace(
            api=SimpleNamespace(api_url=lambda token, method: "http://127.0.0.1:8081/example")
        ),
    )
    message = SimpleNamespace(chat=SimpleNamespace(id=999), message_id=42)
    rules = [Rule(4, "Light Dante")]
    text, kb = rule_catalog_view(rules, folder_id=-1, folder_name="", page=0)
    html, footer = rich_catalog_payload(text, kb, rules)

    ok_client = StubClient({"ok": True, "result": {"message_id": 42}})
    ok = asyncio.run(try_edit_rich_catalog(
        bot=bot, message=message, html=html, footer=footer,
        client_factory=lambda **kw: ok_client,
    ))
    assert ok
    assert ok_client.last_request[1]["rich_message"]["html"] == html

    broken_client = StubClient({
        "ok": False, "error_code": 400,
        "description": "Bad Request: rich_message is not supported",
    })
    fallback = asyncio.run(try_edit_rich_catalog(
        bot=bot, message=message, html=html, footer=footer,
        client_factory=lambda **kw: broken_client,
    ))
    assert not fallback


def test_editor_does_not_log_token_on_transport_error():
    class StubClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def post(self, url, json):
            raise RuntimeError(f"Failed to POST {url}: secret-token")

    class Logger:
        def __init__(self):
            self.messages = []

        def warning(self, fmt, *args):
            self.messages.append(fmt % args)

    logger = Logger()
    bot = SimpleNamespace(
        token="secret-token",
        session=SimpleNamespace(
            api=SimpleNamespace(
                api_url=lambda token, method: "http://localhost/botsecret-token/editMessageText",
            )
        ),
    )
    message = SimpleNamespace(chat=SimpleNamespace(id=1), message_id=2)
    from aiogram.types import InlineKeyboardMarkup
    ok = asyncio.run(try_edit_rich_catalog(
        bot=bot, message=message, html="<p>Hi</p>",
        footer=InlineKeyboardMarkup(inline_keyboard=[]),
        client_factory=lambda **kw: StubClient(),
        logger=logger,
    ))
    assert not ok
    assert logger.messages
    assert "secret-token" not in str(logger.messages)
