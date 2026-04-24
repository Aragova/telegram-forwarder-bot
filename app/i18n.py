from __future__ import annotations

from threading import Lock
from typing import Any

_DEFAULT_LANG = "ru"
_SUPPORTED = {"ru", "en", "es", "de", "pt"}

_LANG_STORAGE: dict[int, str] = {}
_LOCK = Lock()

_MESSAGES: dict[str, dict[str, str]] = {
    "language.changed.ru": {
        "ru": "✅ Язык интерфейса изменён на русский.",
        "en": "✅ Interface language changed to Russian.",
        "es": "✅ El idioma de la interfaz se cambió a ruso.",
        "de": "✅ Die Sprache der Oberfläche wurde auf Russisch geändert.",
        "pt": "✅ O idioma da interface foi alterado para russo.",
    },
    "language.changed.en": {
        "ru": "✅ Язык интерфейса изменён на английский.",
        "en": "✅ Interface language changed to English.",
        "es": "✅ El idioma de la interfaz se cambió a inglés.",
        "de": "✅ Die Sprache der Oberfläche wurde auf Englisch geändert.",
        "pt": "✅ O idioma da interface foi alterado para inglês.",
    },
    "language.changed.es": {
        "ru": "✅ Язык интерфейса изменён на испанский.",
        "en": "✅ Interface language changed to Spanish.",
        "es": "✅ El idioma de la interfaz se cambió a español.",
        "de": "✅ Die Sprache der Oberfläche wurde auf Spanisch geändert.",
        "pt": "✅ O idioma da interface foi alterado para espanhol.",
    },
    "language.changed.de": {
        "ru": "✅ Язык интерфейса изменён на немецкий.",
        "en": "✅ Interface language changed to German.",
        "es": "✅ El idioma de la interfaz se cambió a alemán.",
        "de": "✅ Die Sprache der Oberfläche wurde auf Deutsch geändert.",
        "pt": "✅ O idioma da interface foi alterado para alemão.",
    },
    "language.changed.pt": {
        "ru": "✅ Язык интерфейса изменён на португальский.",
        "en": "✅ Interface language changed to Portuguese.",
        "es": "✅ El idioma de la interfaz se cambió a portugués.",
        "de": "✅ Die Sprache der Oberfläche wurde auf Portugiesisch geändert.",
        "pt": "✅ O idioma da interface foi alterado para português.",
    },
    "language.select": {
        "ru": "🌐 Выберите язык / Choose language",
        "en": "🌐 Choose language / Выберите язык",
        "es": "🌐 Elige idioma",
        "de": "🌐 Sprache wählen",
        "pt": "🌐 Escolha o idioma",
    },
    "owner.mode": {
        "ru": "👑 Режим владельца\n\nТариф: OWNER\nЛимиты: без ограничений",
        "en": "👑 Owner mode\n\nPlan: OWNER\nLimits: unlimited",
        "es": "👑 Modo propietario\n\nPlan: OWNER\nLímites: ilimitado",
        "de": "👑 Eigentümermodus\n\nTarif: OWNER\nLimits: unbegrenzt",
        "pt": "👑 Modo proprietário\n\nPlano: OWNER\nLimites: ilimitado",
    },
}


def normalize_language(lang: str | None) -> str:
    candidate = (lang or "").strip().lower()
    return candidate if candidate in _SUPPORTED else _DEFAULT_LANG


def get_user_language(user_id: int, repo: Any | None = None) -> str:
    uid = int(user_id)
    if repo and hasattr(repo, "get_user_language"):
        try:
            value = repo.get_user_language(uid)
            if value:
                return normalize_language(str(value))
        except Exception:
            pass
    with _LOCK:
        return normalize_language(_LANG_STORAGE.get(uid, _DEFAULT_LANG))


def set_user_language(user_id: int, language_code: str, repo: Any | None = None) -> str:
    uid = int(user_id)
    lang = normalize_language(language_code)
    if repo and hasattr(repo, "set_user_language"):
        try:
            repo.set_user_language(uid, lang)
            return lang
        except Exception:
            pass
    with _LOCK:
        _LANG_STORAGE[uid] = lang
    return lang


def t(key: str, lang: str = "ru", **kwargs: Any) -> str:
    language = normalize_language(lang)
    item = _MESSAGES.get(key)
    if not item:
        return key
    template = item.get(language) or item.get(_DEFAULT_LANG) or key
    return template.format(**kwargs)
