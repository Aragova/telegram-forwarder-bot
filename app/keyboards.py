from __future__ import annotations

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def _kb(
    rows: list[list[str]],
    placeholder: str | None = None,
    one_time: bool = False,
) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=text) for text in row] for row in rows],
        resize_keyboard=True,
        one_time_keyboard=one_time,
        input_field_placeholder=placeholder,
    )


# 🚀 Старт
def get_start_keyboard() -> ReplyKeyboardMarkup:
    return _kb(
        [["📋 Меню"]],
        placeholder="Нажмите 📋 Меню для открытия управления",
    )


# 📋 Главное меню
def get_main_menu() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["📈 Живой статус", "🔄 Правила"],
            ["📡 Каналы", "📦 Очередь"],
            ["⚠️ Диагностика", "⚙️ Система"],
        ],
        placeholder="Выберите действие",
    )

def get_diagnostics_menu() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["⚠️ Проблемные доставки"],
            ["📊 Журнал системы"],
            ["🎨 Тест styled-кнопок"],
            ["⬅️ Назад в меню"],
        ],
        placeholder="Раздел: Диагностика",
    )

def get_system_menu() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["▶️ Запустить пересылку", "⏸ Остановить пересылку"],
            ["💱 Курсы валют"],
            ["⬅️ Назад в меню"],
        ],
        placeholder="Раздел: Система",
    )

# ❌ Отмена
def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    return _kb([["❌ Отмена"]], one_time=True)


# 📤 / 📥 выбор типа канала
def get_channel_type_keyboard() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["📤 Источник"],
            ["📥 Получатель"],
            ["❌ Отмена"],
        ],
        one_time=True,
    )


# 📺 / 👥 выбор сущности
def get_entity_kind_keyboard() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["📺 Канал"],
            ["👥 Группа с темой"],
            ["❌ Отмена"],
        ],
        one_time=True,
    )


# 🔄 Меню правил
def get_rules_menu() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["📜 Список правил"],
            ["➕ Добавить правило"],
            ["⬅️ Назад в меню"],
        ],
        placeholder="Раздел: Правила",
    )

def get_channels_menu() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["📜 Список каналов"],
            ["➕ Добавить канал", "➖ Удалить канал"],
            ["⬅️ Назад в меню"],
        ],
        placeholder="Раздел: Каналы",
    )


# 🔄 Меню сброса
def get_reset_queue_menu() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["🔄 Сбросить всё"],
            ["📊 Сброс по источнику"],
            ["🔙 Главное меню"],
        ]
    )

def get_queue_menu() -> ReplyKeyboardMarkup:
    return _kb(
        [
            ["📋 Общая очередь"],
            ["🔄 Сбросить всё", "📊 Сброс по источнику"],
            ["⬅️ Назад в меню"],
        ],
        placeholder="Раздел: Очередь",
    )

# Универсальный список выбора
def build_select_keyboard(
    items: list[str],
    cancel_text: str = "❌ Отмена",
) -> ReplyKeyboardMarkup:
    rows = [[item] for item in items]
    rows.append([cancel_text])
    return _kb(rows)


# Удаление каналов
def build_channels_remove_keyboard(
    sources_count: int,
    targets_count: int,
) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []

    for idx in range(1, sources_count + 1):
        rows.append([f"Удалить источник {idx}"])

    for idx in range(sources_count + 1, sources_count + targets_count + 1):
        rows.append([f"Удалить получатель {idx}"])

    rows.append(["❌ Отмена"])
    return _kb(rows)


def build_sources_keyboard(labels: list[str]) -> ReplyKeyboardMarkup:
    return build_select_keyboard(labels)


def build_targets_keyboard(labels: list[str]) -> ReplyKeyboardMarkup:
    return build_select_keyboard(labels)


def build_rules_delete_keyboard(labels: list[str]) -> ReplyKeyboardMarkup:
    return build_select_keyboard(labels)


# Сброс по каналам
def build_reset_channels_keyboard(labels: list[str]) -> ReplyKeyboardMarkup:
    rows = [[label] for label in labels]
    rows.append(["🔙 Главное меню"])
    return _kb(rows)
