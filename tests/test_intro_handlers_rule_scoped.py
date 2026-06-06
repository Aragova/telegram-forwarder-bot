from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
INTRO_HANDLERS = REPO_ROOT / "app" / "intro_handlers.py"


def _source() -> str:
    return INTRO_HANDLERS.read_text()


def test_rule_scoped_callback_prefixes_exist():
    source = _source()

    callback_prefixes = [
        "intro_upload:",
        "intro_view:",
        "intro_delete_confirm:",
        "intro_delete_apply:",
        "intro_delete_cancel:",
        "intro_back_to_list:",
        "intro_upload_cancel:",
        "intro_clear_assignment:",
        "apply_intro:",
        "video_intro_menu:",
        "video_intro_horizontal:",
        "video_intro_vertical:",
    ]

    for callback_prefix in callback_prefixes:
        assert callback_prefix in source


def test_global_intro_methods_are_not_used_in_main_flow():
    source = _source()

    assert "add_rule_intro" in source
    assert "list_rule_intros" in source
    assert "get_rule_intro" in source
    assert "soft_delete_rule_intro" in source

    assert "ctx.db.add_intro" not in source
    assert "ctx.db.delete_intro" not in source


def test_upload_state_contains_rule_id_bank_and_rule_flow_marker():
    source = _source()

    assert "intro_upload_wait_file" in source
    assert "rule_id" in source
    assert "bank" in source
    assert "rule_intro_upload" in source


def test_new_bank_callbacks_are_present():
    source = _source()

    assert 'callback_data=f"intro_upload:{rule_id}:{bank}"' in source
    assert 'callback_data=f"intro_view:{rule_id}:{bank}:{intro.id}"' in source
    assert 'callback_data=f"intro_delete_confirm:{rule_id}:{bank}:{intro.id}"' in source
    assert 'callback_data=f"intro_delete_apply:{rule_id}:{bank}:{intro_id}"' in source


def test_apply_intro_checks_bank_ownership_before_assignment():
    source = _source()
    apply_handler = source.split("async def handle_apply_intro", maxsplit=1)[1]
    apply_handler = apply_handler.split("@dp.message", maxsplit=1)[0]

    assert "get_rule_intro" in apply_handler
    assert "bank=mode" in apply_handler
    assert apply_handler.index("get_rule_intro") < apply_handler.index("_apply_intro_sync")


def test_apply_intro_replaces_preview_media_with_text_result():
    source = _source()
    helper_block = source.split("async def _replace_callback_message_with_text", maxsplit=1)[1]
    helper_block = helper_block.split("@dp.callback_query", maxsplit=1)[0]
    apply_handler = source.split("async def handle_apply_intro", maxsplit=1)[1]
    apply_handler = apply_handler.split("@dp.message", maxsplit=1)[0]

    assert "try_delete_message_safe" in helper_block
    assert "send_message_safe" in helper_block
    assert "_replace_callback_message_with_text" in apply_handler
    assert "edit_message_text_safe" not in apply_handler


def test_delete_intro_is_soft_rule_and_bank_scoped():
    source = _source()
    delete_flow = source.split("async def handle_intro_delete_apply", maxsplit=1)[1]
    delete_flow = delete_flow.split("@dp.callback_query(lambda c: c.data.startswith(\"video_intro_horizontal:\"))", maxsplit=1)[0]

    assert "soft_delete_rule_intro" in delete_flow
    assert "bank=bank" in delete_flow
    assert "delete_intro" not in delete_flow.replace("soft_delete_rule_intro", "")


def test_bank_screens_have_required_texts():
    source = _source()

    assert "Горизонтальные заставки правила" in source
    assert "Вертикальные заставки правила" in source
    assert "Здесь хранятся только" in source
    assert "горизонтальные" in source
    assert "вертикальные" in source


def test_root_intro_menu_is_navigation_only():
    source = _source()
    build_block = source[
        source.index("def build_intro_list_keyboard"):
        source.index("def _build_intro_bank_keyboard")
    ]

    assert "🖥 Выбрать горизонтальную" in build_block
    assert "📱 Выбрать вертикальную" in build_block
    assert "➕ Загрузить заставку" not in build_block
    assert "🔄 Обновить список" not in build_block
    assert "👁" not in build_block


def test_preview_uses_contextual_actions_not_both_assignments():
    source = _source()
    preview_block = source.split("async def handle_intro_view", maxsplit=1)[1]
    preview_block = preview_block.split("@dp.callback_query(lambda c: c.data == \"intro_back_to_list\"", maxsplit=1)[0]

    assert "Использовать эту горизонтальную" in preview_block
    assert "Использовать эту вертикальную" in preview_block
    assert "Назначить горизонтальной" not in preview_block
    assert "Назначить вертикальной" not in preview_block


def test_delete_confirm_deletes_media_preview_and_sends_text_message():
    source = _source()
    confirm_block = source.split("async def handle_intro_delete_confirm", maxsplit=1)[1]
    confirm_block = confirm_block.split("@dp.callback_query(lambda c: c.data.startswith(\"intro_delete_cancel:\"))", maxsplit=1)[0]

    assert "_replace_callback_message_with_text" in confirm_block
    assert "edit_message_text_safe" not in confirm_block


def test_intro_preview_can_return_to_rule_intro_bank():
    source = _source()

    assert "intro_back_to_list:{rule_id}:{bank}" in source
    assert "handle_intro_back_to_list" in source


def test_intro_upload_screen_has_cancel_callback():
    source = _source()

    assert "intro_upload_cancel:" in source
    assert "handle_intro_upload_cancel" in source
    assert "❌ Отменить загрузку" in source


def test_intro_upload_cancel_resets_state_to_intro_menu():
    source = _source()
    cancel_block = source.split("async def handle_intro_upload_cancel", maxsplit=1)[1]
    cancel_block = cancel_block.split("@dp.callback_query(lambda c: c.data.startswith(\"intro_view:\"))", maxsplit=1)[0]

    assert '"action": "intro_menu"' in cancel_block
    assert '"intro_upload_wait_file"' in cancel_block


def test_intro_save_errors_separate_duplicate_from_technical_errors():
    source = _source()

    assert "_is_duplicate_intro_error" in source
    assert "Заставка с таким названием уже есть в этом правиле" in source
