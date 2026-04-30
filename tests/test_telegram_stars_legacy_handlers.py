from pathlib import Path


def test_bot_legacy_precheckout_has_stars_guard_before_unconditional_answer():
    bot_text = Path("bot.py").read_text(encoding="utf-8")
    marker = "@dp.pre_checkout_query()\nasync def handle_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):"
    start = bot_text.find(marker)
    assert start != -1, "Legacy pre_checkout handler must exist or test should be updated to explicit removal flow"

    next_handler = bot_text.find("\n\n@dp.", start + 1)
    block = bot_text[start: next_handler if next_handler != -1 else len(bot_text)]

    assert 'payload.startswith("vimi:stars:")' in block
    assert "LEGACY_PRECHECKOUT_SKIP_STARS" in block

    answer_idx = block.find("await pre_checkout_query.answer(ok=True)")
    guard_idx = block.find('payload.startswith("vimi:stars:")')
    assert answer_idx != -1, "Regression: unconditional pre_checkout answer removed unexpectedly"
    assert guard_idx != -1 and guard_idx < answer_idx, "Regression: Stars guard must be before pre_checkout ok=True"


def test_bot_legacy_successful_payment_has_stars_skip():
    bot_text = Path("bot.py").read_text(encoding="utf-8")
    marker = "@dp.message(lambda m: bool(getattr(m, \"successful_payment\", None)))\nasync def handle_successful_payment(message: Message):"
    start = bot_text.find(marker)
    assert start != -1, "Legacy successful_payment handler must exist or test should be updated to explicit removal flow"

    next_handler = bot_text.find("\n\n@dp.", start + 1)
    block = bot_text[start: next_handler if next_handler != -1 else len(bot_text)]

    assert 'payload.startswith("vimi:stars:")' in block
    assert "LEGACY_SUCCESSFUL_PAYMENT_SKIP_STARS" in block


def test_stars_handlers_exist_in_new_flow():
    payments_text = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")

    assert "async def handle_stars_pre_checkout_query" in payments_text
    assert "parse_stars_payload" in payments_text
    assert "async def handle_stars_successful_payment" in payments_text
    assert "message.successful_payment is not None" in payments_text
