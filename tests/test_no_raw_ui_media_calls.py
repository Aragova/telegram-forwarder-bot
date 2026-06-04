from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_UI_PATTERNS = (
    "message.answer_photo",
    "message.answer_video",
    "callback.message.answer_photo",
    "callback.message.answer_video",
    "callback.message.answer_document",
    "ctx.bot.send_photo",
    "ctx.bot.send_document",
)
BOT_WRAPPER_RAW_PATTERNS = (
    "await bot.send_photo",
    "await bot.send_video",
    "await bot.send_document",
    "await bot.copy_message",
    "await bot.forward_message",
)
ALLOWED_BOT_WRAPPERS = {
    "send_photo_safe",
    "send_video_safe",
    "send_document_safe",
    "copy_message_safe",
    "forward_message_safe",
}


def _function_name_for_line(lines: list[str], line_number: int) -> str | None:
    for index in range(line_number - 1, -1, -1):
        line = lines[index]
        if line.startswith("async def ") or line.startswith("def "):
            return line.split("def ", 1)[1].split("(", 1)[0]
    return None


def test_no_raw_ui_media_calls_in_handlers():
    checked_paths = [ROOT / "bot.py"]
    checked_paths.extend((ROOT / "app" / "admin_handlers").glob("*.py"))
    checked_paths.extend((ROOT / "app" / "user_handlers").glob("*.py"))
    checked_paths.extend(ROOT.glob("app/repost_campaign_*handlers.py"))

    violations: list[str] = []
    for path in checked_paths:
        text = path.read_text(encoding="utf-8")
        for pattern in RAW_UI_PATTERNS:
            if pattern in text:
                violations.append(f"{path.relative_to(ROOT)}: {pattern}")

    assert violations == []


def test_bot_raw_media_calls_are_only_safe_wrapper_fallbacks():
    path = ROOT / "bot.py"
    lines = path.read_text(encoding="utf-8").splitlines()
    violations: list[str] = []

    for line_number, line in enumerate(lines, start=1):
        if not any(pattern in line for pattern in BOT_WRAPPER_RAW_PATTERNS):
            continue
        function_name = _function_name_for_line(lines, line_number)
        if function_name not in ALLOWED_BOT_WRAPPERS:
            violations.append(f"bot.py:{line_number}: {line.strip()}")

    assert violations == []
