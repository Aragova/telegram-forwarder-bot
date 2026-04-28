from __future__ import annotations

from dataclasses import dataclass

from app.config import settings


@dataclass(frozen=True, slots=True)
class Tariff:
    code: str
    title_ru: str
    price: float
    currency: str
    lava_offer_id: str
    description_ru: str


def _build_basic_tariff() -> Tariff:
    return Tariff(
        code="basic",
        title_ru="BASIC",
        price=9.0,
        currency="USD",
        lava_offer_id=(
            settings.lava_top_basic_offer_id.strip()
            or "16731707-ffba-466a-80a5-2d4002f33c64"
        ),
        description_ru="Базовый тариф ViMi для подключения автоматизации Telegram-канала.",
    )


def get_tariff(code: str) -> Tariff:
    normalized = str(code or "").strip().lower()
    if normalized == "basic":
        return _build_basic_tariff()
    raise ValueError(f"Неизвестный тариф: {code}")
