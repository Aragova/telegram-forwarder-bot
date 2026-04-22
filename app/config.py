from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@dataclass(slots=True)
class Settings:
    # --- основной бот / Telegram ---
    bot_token: str = os.getenv("BOT_TOKEN", "")
    admin_id: int = int(os.getenv("ADMIN_ID", "0") or 0)
    api_id: int = int(os.getenv("API_ID", "0") or 0)
    api_hash: str = os.getenv("API_HASH", "")
    bot_api_base: str = os.getenv("BOT_API_BASE", "http://127.0.0.1:8081").strip()
    phone_number: str = os.getenv("PHONE_NUMBER", "")

    # --- база / сессии / логи ---
    session_name: str = os.getenv("SESSION_NAME", "parser_session")
    reaction_sessions_raw: str = os.getenv("REACTION_SESSIONS", "")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # --- директории ---
    base_dir_raw: str = os.getenv("BASE_DIR", ".")
    media_dir_raw: str = os.getenv("MEDIA_DIR", "media")
    media_cache_dir: str = os.getenv("MEDIA_CACHE_DIR", "media_cache")
    intros_dir_raw: str = os.getenv("INTROS_DIR", "media/intros")
    temp_dir_raw: str = os.getenv("TEMP_DIR", "media/temp")

    # --- backend базы данных ---
    data_read_backend: str = os.getenv("DATA_READ_BACKEND", "sqlite").strip().lower()

    # --- параметры видеоредактора ---
    intro_duration: int = int(os.getenv("INTRO_DURATION", "2") or 2)
    max_video_duration: int = int(os.getenv("MAX_VIDEO_DURATION", "120") or 120)

    min_free_space_gb: float = float(os.getenv("MIN_FREE_SPACE_GB", "0.5") or 0.5)
    max_temp_size_gb: float = float(os.getenv("MAX_TEMP_SIZE_GB", "5") or 5)
    max_concurrent_jobs: int = int(os.getenv("MAX_CONCURRENT_JOBS", "2") or 2)

    target_width: int = int(os.getenv("TARGET_WIDTH", "1280") or 1280)
    target_height: int = int(os.getenv("TARGET_HEIGHT", "720") or 720)
    target_fps: int = int(os.getenv("TARGET_FPS", "30") or 30)
    target_sample_rate: int = int(os.getenv("TARGET_SAMPLE_RATE", "48000") or 48000)
    target_channels: int = int(os.getenv("TARGET_CHANNELS", "2") or 2)
    target_pix_fmt: str = os.getenv("TARGET_PIX_FMT", "yuv420p")

    max_video_bitrate: str = os.getenv("MAX_VIDEO_BITRATE", "5M")
    video_bufsize: str = os.getenv("VIDEO_BUFSIZE", "10M")

    max_input_duration: int = int(os.getenv("MAX_INPUT_DURATION", "3600") or 3600)
    max_input_bitrate_mbps: int = int(os.getenv("MAX_INPUT_BITRATE_MBPS", "50") or 50)

    def validate(self) -> None:
        missing = []
        for key in ("bot_token", "admin_id", "api_id", "api_hash", "phone_number"):
            if not getattr(self, key):
                missing.append(key.upper())
        if missing:
            raise RuntimeError("Не заполнены переменные окружения: " + ", ".join(missing))

        if self.data_read_backend not in ("sqlite", "postgres"):
            raise RuntimeError(
                "Некорректная переменная DATA_READ_BACKEND. "
                "Допустимо только: sqlite или postgres"
            )

    @property
    def reaction_sessions(self) -> list[str]:
        raw = (self.reaction_sessions_raw or "").strip()
        if not raw:
            return []
        return [item.strip() for item in raw.split(",") if item.strip()]

    @property
    def base_dir(self) -> Path:
        p = Path(self.base_dir_raw)
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def media_dir(self) -> Path:
        raw = Path(self.media_dir_raw)
        p = raw if raw.is_absolute() else self.base_dir / raw
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def media_cache_path(self) -> Path:
        raw = Path(self.media_cache_dir)
        p = raw if raw.is_absolute() else self.base_dir / raw
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def intros_dir(self) -> Path:
        raw = Path(self.intros_dir_raw)
        p = raw if raw.is_absolute() else self.base_dir / raw
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def temp_dir(self) -> Path:
        raw = Path(self.temp_dir_raw)
        p = raw if raw.is_absolute() else self.base_dir / raw
        p.mkdir(parents=True, exist_ok=True)
        return p


settings = Settings()
