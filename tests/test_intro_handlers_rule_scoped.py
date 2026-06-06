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
