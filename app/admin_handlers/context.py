from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(slots=True)
class AdminHandlersContext:
    bot: Any
    db: Any
    scheduler_service: Any
    sender_service: Any
    runtime_context: Any
    user_states: dict[int, dict[str, Any]]
    dashboard_tasks: dict[int, Any]
    run_db: Callable[..., Any]
    is_admin: Callable[..., Any]
    is_admin_callback: Callable[..., Any]
    answer_callback_safe: Callable[..., Any]
    answer_callback_safe_once: Callable[..., Any]
    send_message_safe: Callable[..., Any]
    get_main_menu: Callable[..., Any]
    logger: Any

    reset_user_state: Callable[[int | None], None] | None = None
    ensure_rule_workers: Callable[..., Any] | None = None
    stop_all_workers: Callable[..., Any] | None = None
    parse_callback_parts: Callable[..., Any] | None = None
    clamp_page: Callable[..., int] | None = None
    build_faulty_pages: Callable[..., Any] | None = None
    build_faulty_inline_keyboard: Callable[..., Any] | None = None
    build_system_journal_pages: Callable[..., Any] | None = None
    build_system_journal_inline_keyboard: Callable[..., Any] | None = None
    edit_message_text_safe: Callable[..., Any] | None = None
    get_channels_menu: Callable[..., Any] | None = None
    get_queue_menu: Callable[..., Any] | None = None
    get_diagnostics_menu: Callable[..., Any] | None = None
    get_system_menu: Callable[..., Any] | None = None
    get_rules_menu: Callable[..., Any] | None = None
    get_reset_queue_menu: Callable[..., Any] | None = None
    get_channel_type_keyboard: Callable[..., Any] | None = None
    get_entity_kind_keyboard: Callable[..., Any] | None = None
    get_cancel_keyboard: Callable[..., Any] | None = None
    reply_keyboard_markup_cls: Any = None
    keyboard_button_cls: Any = None
    channel_choice_cls: Any = None
    settings: Any = None
    ensure_user_tenant: Callable[..., Any] | None = None
    is_admin_user: Callable[..., bool] | None = None
    parse_channel_history: Callable[..., Any] | None = None
    telethon_client: Any = None
    show_public_user_menu_message: Callable[..., Any] | None = None
    start_forwarding: Callable[..., Any] | None = None
    stop_forwarding: Callable[..., Any] | None = None
    is_posting_active: Callable[[], bool] | None = None
