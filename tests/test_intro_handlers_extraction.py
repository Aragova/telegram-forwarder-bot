from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_intro_handlers_module_exists():
    import app.intro_handlers  # noqa: F401


def test_intro_handlers_registration_exists():
    from app.intro_handlers import IntroHandlersContext, register_intro_handlers

    assert IntroHandlersContext is not None
    assert callable(register_intro_handlers)


def test_bot_py_no_intro_handler_definitions():
    bot_py = (REPO_ROOT / "bot.py").read_text()

    removed_definitions = [
        "handle_video_intro_menu",
        "handle_intro_upload",
        "handle_intro_file",
        "handle_apply_intro",
    ]

    for name in removed_definitions:
        assert f"def {name}" not in bot_py
        assert f"async def {name}" not in bot_py


def test_callback_prefixes_are_preserved():
    intro_handlers_py = (REPO_ROOT / "app" / "intro_handlers.py").read_text()

    preserved_callback_data = [
        "video_intro_menu:",
        "user_rule_intros:",
        "intro_upload:",
        "intro_view:",
        "intro_delete_confirm:",
        "intro_delete_apply:",
        "intro_delete_cancel:",
        "intro_back_to_list:",
        "video_intro_horizontal:",
        "video_intro_vertical:",
        "apply_intro:",
    ]

    for callback_data in preserved_callback_data:
        assert callback_data in intro_handlers_py
