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


def test_upload_state_contains_rule_id_and_rule_flow_marker():
    source = _source()

    assert "intro_upload_wait_file" in source
    assert "rule_id" in source
    assert "rule_intro_upload" in source


def test_apply_intro_checks_rule_ownership_before_assignment():
    source = _source()
    apply_handler = source.split("async def handle_apply_intro", maxsplit=1)[1]
    apply_handler = apply_handler.split("@dp.message", maxsplit=1)[0]

    assert "get_rule_intro" in apply_handler
    assert apply_handler.index("get_rule_intro") < apply_handler.index("_apply_intro_sync")


def test_delete_intro_is_soft_and_rule_scoped():
    source = _source()
    delete_flow = source.split("async def handle_intro_delete_apply", maxsplit=1)[1]
    delete_flow = delete_flow.split("@dp.callback_query(lambda c: c.data.startswith(\"video_intro_horizontal:\"))", maxsplit=1)[0]

    assert "soft_delete_rule_intro" in delete_flow
    assert "delete_intro" not in delete_flow.replace("soft_delete_rule_intro", "")


def test_empty_state_mentions_no_rule_intros():
    source = _source()

    assert "В этом правиле пока нет загруженных заставок" in source


def test_ui_no_longer_promises_global_bank():
    source = _source()

    assert "доступны только этому правилу" in source or "только этого правила" in source
    assert "Всего заставок" not in source


def test_intro_main_keyboard_has_refresh_instead_of_my_intros():
    source = _source()

    assert "📦 Мои заставки" not in source
    assert "🔄 Обновить список" in source
    assert 'callback_data=f"video_intro_menu:{rule_id}"' in source


def test_intro_main_keyboard_does_not_use_preview_back_callback():
    source = _source()
    build_block = source[
        source.index("def build_intro_list_keyboard"):
        source.index("def _build_intro_selection_keyboard")
    ]

    assert "intro_back_to_list" not in build_block


def test_intro_preview_can_still_return_to_rule_intro_list():
    source = _source()

    assert "intro_back_to_list:{rule_id}" in source
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
    assert "Не удалось сохранить заставку" in source
