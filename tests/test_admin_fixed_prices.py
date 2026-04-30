from pathlib import Path


def test_fixed_price_state_excluded_from_global_stateful_handler():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "admin_fixed_price_input" in source
    assert (
        'not in {"admin_billing_rate_input", "admin_billing_usd_price_input", "admin_fixed_price_input"}'
        in source
    )


def test_bot_has_no_fixed_price_persistence_logic():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "set_billing_fixed_price" not in source


def test_system_handler_owns_fixed_price_input_and_persistence():
    source = Path("app/admin_handlers/system.py").read_text(encoding="utf-8")
    assert '"action") == "admin_fixed_price_input"' in source
    assert "handle_fixed_price_input" in source
    assert "set_billing_fixed_price" in source
