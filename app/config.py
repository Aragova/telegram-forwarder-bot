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
    # LEGACY: в текущей архитектуре repository_factory всегда использует PostgresRepository.
    # Флаг сохранён только для обратной совместимости окружений и не влияет на выбор репозитория.
    data_read_backend: str = os.getenv("DATA_READ_BACKEND", "postgres").strip().lower()

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

    # --- ресурсная модель workers / throughput ---
    light_max_concurrency: int = int(os.getenv("LIGHT_MAX_CONCURRENCY", "4") or 4)
    heavy_max_concurrency: int = int(os.getenv("HEAVY_MAX_CONCURRENCY", "2") or 2)
    heavy_download_max_concurrency: int = int(os.getenv("HEAVY_DOWNLOAD_MAX_CONCURRENCY", "1") or 1)
    heavy_process_max_concurrency: int = int(os.getenv("HEAVY_PROCESS_MAX_CONCURRENCY", "1") or 1)
    heavy_send_max_concurrency: int = int(os.getenv("HEAVY_SEND_MAX_CONCURRENCY", "1") or 1)
    lease_batch_size_light: int = int(os.getenv("LEASE_BATCH_SIZE_LIGHT", "4") or 4)
    lease_batch_size_heavy: int = int(os.getenv("LEASE_BATCH_SIZE_HEAVY", "2") or 2)
    backlog_soft_limit_light: int = int(os.getenv("BACKLOG_SOFT_LIMIT_LIGHT", "200") or 200)
    backlog_soft_limit_heavy: int = int(os.getenv("BACKLOG_SOFT_LIMIT_HEAVY", "100") or 100)
    backlog_hard_limit_heavy: int = int(os.getenv("BACKLOG_HARD_LIMIT_HEAVY", "250") or 250)
    max_heavy_retries_in_flight: int = int(os.getenv("MAX_HEAVY_RETRIES_IN_FLIGHT", "20") or 20)
    graceful_shutdown_timeout_sec: int = int(os.getenv("GRACEFUL_SHUTDOWN_TIMEOUT_SEC", "20") or 20)

    # --- payment layer ---
    payment_enabled: bool = str(os.getenv("PAYMENT_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
    payment_default_provider: str = os.getenv("PAYMENT_DEFAULT_PROVIDER", "manual_bank_card").strip().lower()
    payment_allowed_providers_raw: str = os.getenv("PAYMENT_ALLOWED_PROVIDERS", "").strip()

    paypal_enabled: bool = str(os.getenv("PAYPAL_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
    paypal_client_id: str = os.getenv("PAYPAL_CLIENT_ID", "")
    paypal_client_secret: str = os.getenv("PAYPAL_CLIENT_SECRET", "")
    paypal_env: str = os.getenv("PAYPAL_ENV", "sandbox").strip().lower()
    paypal_webhook_id: str = os.getenv("PAYPAL_WEBHOOK_ID", "")

    telegram_stars_enabled: bool = str(os.getenv("TELEGRAM_STARS_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
    telegram_payments_enabled: bool = str(os.getenv("TELEGRAM_PAYMENTS_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
    telegram_payment_provider_token: str = os.getenv("TELEGRAM_PAYMENT_PROVIDER_TOKEN", "")

    manual_card_enabled: bool = str(os.getenv("MANUAL_CARD_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
    manual_card_text_ru: str = os.getenv("MANUAL_CARD_TEXT_RU", "💳 Переведите сумму по реквизитам и нажмите «Я оплатил».")
    manual_card_text_en: str = os.getenv("MANUAL_CARD_TEXT_EN", "💳 Transfer the amount by bank card details and press “I have paid”.")

    sbp_manual_enabled: bool = str(os.getenv("SBP_MANUAL_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
    sbp_payment_text_ru: str = os.getenv("SBP_PAYMENT_TEXT_RU", "⚡ Оплатите через СБП и нажмите «Я оплатил».")
    sbp_payment_text_en: str = os.getenv("SBP_PAYMENT_TEXT_EN", "⚡ Pay via fast payment system and press “I have paid”.")

    crypto_manual_enabled: bool = str(os.getenv("CRYPTO_MANUAL_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
    crypto_usdt_trc20_address: str = os.getenv("CRYPTO_USDT_TRC20_ADDRESS", "")
    crypto_usdt_ton_address: str = os.getenv("CRYPTO_USDT_TON_ADDRESS", "")
    crypto_btc_address: str = os.getenv("CRYPTO_BTC_ADDRESS", "")

    tribute_enabled: bool = str(os.getenv("TRIBUTE_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
    tribute_api_key: str = os.getenv("TRIBUTE_API_KEY", "")
    tribute_webhook_secret: str = os.getenv("TRIBUTE_WEBHOOK_SECRET", "")

    lava_top_enabled: bool = str(os.getenv("LAVA_TOP_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
    lava_top_api_key: str = os.getenv("LAVA_TOP_API_KEY", "")
    lava_top_api_base: str = os.getenv("LAVA_TOP_API_BASE", "https://gate.lava.top").rstrip("/")
    lava_top_webhook_secret: str = os.getenv("LAVA_TOP_WEBHOOK_SECRET", "")
    lava_top_basic_offer_id: str = os.getenv("LAVA_TOP_BASIC_OFFER_ID", "").strip()

    public_base_url: str = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

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
                "Допустимо только: sqlite или postgres (legacy-параметр, репозиторий всё равно Postgres)"
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

    @property
    def payment_allowed_providers(self) -> list[str]:
        raw = self.payment_allowed_providers_raw.strip()
        if not raw:
            return []
        return [item.strip().lower() for item in raw.split(",") if item.strip()]


settings = Settings()
