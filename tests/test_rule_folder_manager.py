from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from app.rule_folders_repository import RuleFoldersRepositoryMixin, normalize_folder_name
from app import rule_folder_handlers as folder_handlers
from app.rule_folders_ui import (
    additional_functions_view,
    folder_delete_confirmation_view,
    folder_index_view,
    move_picker_view,
    root_view,
    rule_catalog_view,
    rule_matches_query,
)


@dataclass
class FakeRule:
    id: int
    source_id: str = "-1001"
    source_thread_id: int | None = None
    target_id: str = "-1002"
    target_thread_id: int | None = None
    source_title: str = "Длинное название источника публичного канала с полным описанием"
    target_title: str = "Очень длинное название приватного получателя за звёзды Telegram"
    is_active: bool = True


def callbacks(markup):
    return [b.callback_data for row in markup.inline_keyboard for b in row]


def test_root_hides_empty_folders_until_first_created():
    text, kb = root_view(total_rules=298, folders_count=0, admin=True)
    assert "298" in text
    assert "📁 Мои папки" not in str(kb)
    assert "rf:create" in callbacks(kb)
    _, kb = root_view(total_rules=298, folders_count=1, admin=True)
    assert "rf:folders:0" in callbacks(kb)


def test_catalog_preserves_full_titles_in_message_not_inline_buttons():
    rules = [FakeRule(100 + i) for i in range(12)]
    text, kb = rule_catalog_view(rules, folder_id=-1, folder_name="", page=0)
    assert "Длинное название источника публичного канала с полным описанием" in text
    assert "Очень длинное название приватного получателя за звёзды Telegram" in text
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert all("название источника" not in label for label in labels)
    assert "rf:list:-1:1" in callbacks(kb)
    assert len([cb for cb in callbacks(kb) if cb and cb.startswith("rf:open:")]) == 5


def test_search_results_return_to_correct_page():
    rules = [FakeRule(i) for i in range(10)]
    text, kb = rule_catalog_view(rules, folder_id=-1, folder_name="", page=1,
                                 search_term="длинное")
    assert "РЕЗУЛЬТАТЫ ПОИСКА" in text
    assert "rf:open_search:5:1" in callbacks(kb)
    assert "rf:results:0" in callbacks(kb)


def test_rich_extra_has_two_column_actions_and_grouped_explanations():
    text, kb = additional_functions_view(
        rule_id=214, target_title="<приватный>", mode="repost",
        folder_name="⭐ Stars & VIP", admin=True,
    )
    assert "&lt;приватный&gt;" in text
    assert "⭐ Stars &amp; VIP" in text
    assert "<b>ОБСЛУЖИВАНИЕ</b>" in text
    assert "rf:move:214" in callbacks(kb)
    assert "repost_caption_menu:214" in callbacks(kb)
    assert any(len(row) == 2 for row in kb.inline_keyboard)
    assert "delete_rule:214" in callbacks(kb)


def test_tenant_extra_retains_reactions_and_video_mode():
    text, kb = additional_functions_view(
        rule_id=4, target_title="Видео", mode="video", folder_name=None,
        admin=False,
    )
    assert "✍️ <b>Подпись</b>" in text
    assert "Длина видео" not in text
    assert "Без папки" in text
    assert "user_rule_caption:4" in callbacks(kb)
    assert "user_rule_reactions:4" in callbacks(kb)
    assert "user_rule_rollback:4" in callbacks(kb)


def test_folders_and_move_picker_use_short_buttons_and_full_names():
    folders = [
        {"id": 1, "name": "🪄 " + "длинное название " * 3, "rules_count": 22},
        {"id": 2, "name": "Приватные", "rules_count": 10},
    ]
    text, kb = folder_index_view(folders, 0)
    assert folders[0]["name"] in text
    assert "rf:list:1:0" in callbacks(kb)
    assert all(len(btn.text) < 50 for row in kb.inline_keyboard for btn in row)
    text2, kb2 = move_picker_view(folders=folders, active_name="Публичные",
                                  origin="s214", rule_count=1)
    assert "Публикации и очередь не изменятся" in text2
    assert "rf:apply:s214:1" in callbacks(kb2)
    assert "rf:apply:s214:0" in callbacks(kb2)


def test_folder_delete_is_explicitly_non_destructive():
    text, kb = folder_delete_confirmation_view({"id": 2, "name": "Stars"})
    assert "Сами правила" in text
    assert "rf:delete:2" in callbacks(kb)


@pytest.mark.parametrize("name", [" ", "\n", "x" * 65])
def test_folder_names_reject_blank_or_overlength(name):
    with pytest.raises(ValueError):
        normalize_folder_name(name)


def test_folder_names_collapse_whitespace():
    assert normalize_folder_name("  Платные \n каналы  ") == "Платные каналы"


class _Cursor:
    def __init__(self, *, owned=2, folder_exists=True):
        self.owned = owned
        self.folder_exists = folder_exists
        self.queries = []
        self._last = ""
        self.rowcount = 1

    def execute(self, query, params=None):
        self._last = str(query)
        self.queries.append((str(query), params))

    def fetchone(self):
        if "SELECT id FROM rule_folders" in self._last:
            return {"id": 10} if self.folder_exists else None
        return {"cnt": self.owned}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class _Conn:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class _FakeFolderRepo(RuleFoldersRepositoryMixin):
    def __init__(self, *, owned=2, folder_exists=True):
        self.connection = _Conn(_Cursor(owned=owned, folder_exists=folder_exists))

    def connect(self):
        return self.connection


def test_bulk_move_is_tenant_scoped_and_does_not_touch_delivery_state():
    repo = _FakeFolderRepo()
    assert repo.assign_rules_to_folder(4, [214, 215], 10) == 2
    assert repo.connection.commits == 1
    statements = " ".join(q for q, _ in repo.connection.cursor_obj.queries).upper()
    assert "COALESCE(TENANT_ID, 1) = %S" in statements
    assert "ON CONFLICT (RULE_ID)" in statements
    assert not any(w in statements for w in ("UPDATE ROUTING", "DELETE FROM DELIVERIES"))


def test_bulk_move_rejects_cross_tenant_and_missing_folder_atomically():
    repo = _FakeFolderRepo(owned=1)
    with pytest.raises(ValueError, match="не принадлежат"):
        repo.assign_rules_to_folder(4, [214, 215], 10)
    assert repo.connection.commits == 0
    assert not any("INSERT INTO rule_folder_assignments" in sql
                   for sql, _ in repo.connection.cursor_obj.queries)

    repo = _FakeFolderRepo(folder_exists=False)
    with pytest.raises(ValueError, match="Папка не найдена"):
        repo.assign_rules_to_folder(4, [214, 215], 10)
    assert repo.connection.commits == 0


def test_move_to_unfiled_never_deletes_the_rule():
    repo = _FakeFolderRepo(owned=2)
    assert repo.assign_rules_to_folder(4, [214, 215], None) == 2
    sql = " ".join(q for q, _ in repo.connection.cursor_obj.queries)
    assert "DELETE FROM rule_folder_assignments" in sql
    assert "DELETE FROM routing" not in sql


def test_hash_prefixed_rule_search_finds_exact_id_and_not_partial_id():
    assert rule_matches_query(FakeRule(214), "#214")
    assert rule_matches_query(FakeRule(214), "# 214")
    assert not rule_matches_query(FakeRule(1214), "#214")
    assert not rule_matches_query(FakeRule(214), "#215")
    assert rule_matches_query(FakeRule(214), "длинное название")
    assert rule_matches_query(FakeRule(214), "-1002")
    assert not rule_matches_query(FakeRule(214), "")


def test_new_folder_picker_cancel_returns_to_same_picker_page():
    folders = [{"id": i, "name": f"Папка {i}", "rules_count": 1} for i in range(18)]
    _, kb = move_picker_view(folders=folders, active_name="Приватные",
                             origin="s214", rule_count=1, page=1)
    assert "rf:create_for:s214:1" in callbacks(kb)
    assert "rf:move_back:s214" in callbacks(kb)
    prompt, back = folder_handlers._input_prompt("create", origin="s214", page=1)
    assert "НОВАЯ ПАПКА" in prompt
    assert back == "rf:move_page:s214:1"


def test_cancel_pending_folder_input_does_not_keep_a_rename_wizard():
    uid = 785001
    folder_handlers._inputs[uid] = {"kind": "rename", "folder_id": 7}
    folder_handlers.clear_folder_input(uid)
    assert uid not in folder_handlers._inputs


def test_live_admin_reply_keyboard_opens_folder_root_and_not_legacy_reply_menu():
    source = Path("bot.py").read_text(encoding="utf-8")
    start = source.index("async def handle_admin_reply_keyboard_router(")
    end = source.index("@dp.callback_query", start)
    block = source[start:end]
    assert 'if canonical == "🔄 Правила":' in block
    assert "await show_admin_rule_manager(message, message.from_user.id)" in block
    assert '"🔄 Раздел: Правила", reply_markup=get_rules_menu()' not in block


def test_folder_input_uses_safe_message_edits_and_private_chat_guard():
    source = Path("app/rule_folder_handlers.py").read_text(encoding="utf-8")
    assert 'getattr(m.chat, "type", None) == "private"' in source
    assert 'ctx.edit_message_text_safe(' in source
    assert 'ctx.try_delete_message_safe(' in source
