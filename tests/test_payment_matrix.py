from app.payments import payment_matrix


def test_stars_toggle_and_order(monkeypatch):
    monkeypatch.setattr(payment_matrix.settings, "payment_enabled", False)
    monkeypatch.setattr(payment_matrix.settings, "telegram_stars_enabled", True)
    assert payment_matrix.method_by_code("USD", "stars").get("enabled") is False

    monkeypatch.setattr(payment_matrix.settings, "payment_enabled", True)
    monkeypatch.setattr(payment_matrix.settings, "telegram_stars_enabled", False)
    assert payment_matrix.method_by_code("USD", "stars").get("enabled") is False

    monkeypatch.setattr(payment_matrix.settings, "payment_enabled", True)
    monkeypatch.setattr(payment_matrix.settings, "telegram_stars_enabled", True)
    methods = payment_matrix.methods_for_currency("USD")
    assert payment_matrix.method_by_code("USD", "stars").get("enabled") is True
    assert [m["code"] for m in methods][:2] == ["lava_card_usd", "lava_paypal_usd"]
