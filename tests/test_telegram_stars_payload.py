from app.payments.telegram_stars_service import build_stars_payload, parse_stars_payload


def test_payload_build_and_parse():
    raw = build_stars_payload(payment_intent_id=456, invoice_id=123, user_id=789, nonce="abc")
    assert raw == "vimi:stars:pi:456:inv:123:user:789:abc"
    parsed = parse_stars_payload(raw)
    assert parsed.payment_intent_id == 456
    assert parsed.invoice_id == 123
    assert parsed.user_id == 789


def test_payload_invalid_cases():
    for raw in ["", "foo", "vimi:stars:pi:0:inv:1:user:1:abc", "vimi:stars:pi:1:inv:0:user:1:abc", "vimi:stars:pi:1:inv:1:user:0:abc", "vimi:stars:pi:1:inv:1:user:1:"]:
        try:
            parse_stars_payload(raw)
            assert False
        except ValueError:
            assert True
