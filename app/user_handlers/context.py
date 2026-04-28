from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(slots=True)
class UserHandlersContext:
    bot: Any
    db: Any
    tenant_service: Any
    subscription_service: Any
    usage_service: Any
    limit_service: Any
    invoice_service: Any
    billing_service: Any
    payment_service: Any
    recovery_service: Any
    user_states: dict[int, dict[str, Any]]
    run_db: Callable[..., Any]
    answer_callback_safe: Callable[..., Any]
    send_message_safe: Callable[..., Any]
    is_admin_user: Callable[[int | None], bool]
    ensure_user_tenant: Callable[[int], int]
    is_rule_owned_by_user: Callable[[int, int], bool]
    logger: Any

    # Shared low-level helpers from bot.py.
    answer_callback_safe_once: Callable[..., Any] | None = None
    edit_message_text_safe: Callable[..., Any] | None = None
    edit_message_reply_markup_safe: Callable[..., Any] | None = None
    with_recovery_button: Callable[..., Any] | None = None
    recovery_has_items: Callable[..., bool] | None = None
    rules_page_size: int = 8

    get_user_invoices_payload: Callable[..., Any] | None = None
    invoice_plan_name: Callable[..., Any] | None = None
    public_invoice_keyboard: Callable[..., Any] | None = None
    public_plans_keyboard: Callable[..., Any] | None = None
    public_usage_keyboard: Callable[..., Any] | None = None
    get_plan_info: Callable[..., Any] | None = None
    is_subscription_blocked_status: Callable[..., bool] | None = None
    write_billing_event: Callable[..., Any] | None = None
    find_active_manual_payment_intent_for_invoice: Callable[..., Any] | None = None
    find_latest_payment_intent_for_invoice: Callable[..., Any] | None = None
    is_supported_receipt_document: Callable[..., bool] | None = None
    is_admin_callback: Callable[..., Any] | None = None
    utc_now_iso: Callable[[], str] | None = None
    settings: Any = None
    channel_choice_cls: Any = None
    build_rule_card_payload_cached: Callable[..., Any] | None = None
    build_rule_extra_keyboard: Callable[..., Any] | None = None
    compact_rule_text: Callable[..., str] | None = None
    manual_payment_providers: set[str] | None = None
    manual_payment_active_statuses: set[str] | None = None
