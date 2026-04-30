from __future__ import annotations

CRYPTO_WALLETS = [
    {
        "code": "btc",
        "title": "BTC Bitcoin",
        "wallet_label": "Wallet address",
        "wallet_address": "1MsgkYc918tJMkbnAxXfCJRqpPZrVL3GYN",
    },
    {
        "code": "usdt_trc20",
        "title": "USDT Tether TRC20",
        "wallet_label": "Wallet address",
        "wallet_address": "TDfU79B6RwVL7DpKHZu1GjSWYSKY39JDgf",
    },
    {
        "code": "eth",
        "title": "ETH Ethereum",
        "wallet_label": "Wallet address",
        "wallet_address": "0xa25ee4e00c2b578afedfccb5e2c90a996ee21cdd",
    },
    {
        "code": "trx",
        "title": "TRX Tron TRC20",
        "wallet_label": "Wallet address TRX Tron TRC20",
        "wallet_address": "TDfU79B6RwVL7DpKHZu1GjSWYSKY39JDgf",
    },
    {
        "code": "ton",
        "title": "TON Toncoin",
        "wallet_label": "Wallet address",
        "wallet_address": "UQDI5eY8YaVLgWVJjX-iMoXBroNnmSL2S4WsYZopK9cM0CIF",
    },
]


def list_crypto_wallets() -> list[dict[str, str]]:
    return list(CRYPTO_WALLETS)


def get_crypto_wallet(code: str) -> dict[str, str] | None:
    code_value = str(code or "").lower()
    for wallet in CRYPTO_WALLETS:
        if str(wallet.get("code") or "").lower() == code_value:
            return wallet
    return None
