from __future__ import annotations

from threading import Lock
from typing import Any

_DEFAULT_LANG = "ru"
_SUPPORTED = {"ru", "en", "es", "de", "pt", "uk"}

_LANG_STORAGE: dict[int, str] = {}
_LOCK = Lock()

_MESSAGES: dict[str, dict[str, str]] = {
    "language.changed.ru": {
        "ru": "✅ Язык интерфейса изменён на русский.",
        "en": "✅ Interface language changed to Russian.",
    },
    "language.changed.en": {
        "ru": "✅ Язык интерфейса изменён на английский.",
        "en": "✅ Interface language changed to English.",
    },
    "language.select": {
        "ru": "🌐 Выберите язык / Choose language",
        "en": "🌐 Choose language / Выберите язык",
        "es": "🌐 Elige idioma / Выберите язык",
        "de": "🌐 Sprache wählen / Выберите язык",
        "pt": "🌐 Escolha o idioma / Выберите язык",
    },
    "owner.mode": {
        "ru": "👑 Режим владельца\n\nТариф: OWNER\nЛимиты: без ограничений",
        "en": "👑 Owner mode\n\nPlan: OWNER\nLimits: unlimited",
        "es": "👑 Modo propietario\n\nPlan: OWNER\nLímites: ilimitados",
        "de": "👑 Eigentümermodus\n\nPlan: OWNER\nLimits: unbegrenzt",
        "pt": "👑 Modo proprietário\n\nPlano: OWNER\nLimites: ilimitados",
    },
    "language.changed.es": {
        "ru": "✅ Язык интерфейса изменён на испанский.",
        "en": "✅ Interface language changed to Spanish.",
        "es": "✅ Idioma de la interfaz cambiado a español.",
    },
    "language.changed.de": {
        "ru": "✅ Язык интерфейса изменён на немецкий.",
        "en": "✅ Interface language changed to German.",
        "de": "✅ Oberflächensprache auf Deutsch geändert.",
    },
    "language.changed.pt": {
        "ru": "✅ Язык интерфейса изменён на португальский.",
        "en": "✅ Interface language changed to Portuguese.",
        "pt": "✅ Idioma da interface alterado para português.",
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
