from app.payments.fixed_prices import get_stars_price


class Repo:
    def __init__(self, fixed):
        self.fixed = fixed

    def get_billing_fixed_prices(self, kind):
        return self.fixed.get(kind)


def test_get_stars_price_ok_and_missing():
    repo = Repo({"stars": {"basic": {"1": {"stars": 500}}, "pro": {1: {"amount": 1500}}}})
    assert get_stars_price("basic", 1, repo=repo) == 500
    assert get_stars_price("pro", 1, repo=repo) == 1500
    assert get_stars_price("basic", 3, repo=repo) is None


def test_get_stars_price_invalid_values():
    repo = Repo({"stars": {"basic": {1: {"amount": "bad"}, 3: {"amount": 0}}}})
    assert get_stars_price("basic", 1, repo=repo) is None
    assert get_stars_price("basic", 3, repo=repo) is None
