from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from .context import UserHandlersContext
from app import user_ui


def build_user_rules_keyboard(rules, page: int = 0, *, rules_page_size: int = 8, compact_rule_text=None) -> InlineKeyboardMarkup:
    total = len(rules)
    if total == 0:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить правило", callback_data="user_rules_add")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
            ]
        )
    max_page = (total - 1) // rules_page_size
    page = max(0, min(page, max_page))
    start = page * rules_page_size
    current = rules[start:start + rules_page_size]
    rows: list[list[InlineKeyboardButton]] = []
    for row in current:
        rid = int(getattr(row, "id", 0))
        label = compact_rule_text(row) if compact_rule_text else f"{rid}"
        state = "▶️" if bool(getattr(row, "is_active", False)) else "⏸"
        rows.append([InlineKeyboardButton(text=f"{state} #{rid} {label}", callback_data=f"user_rule_open:{rid}")])
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"user_rules_page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{max_page + 1}", callback_data="user_rules_noop"))
    if page < max_page:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"user_rules_page:{page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="➕ Добавить правило", callback_data="user_rules_add")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def build_user_rule_card_payload(ctx: UserHandlersContext, rule_id: int) -> tuple[str | None, InlineKeyboardMarkup | None]:
    snapshot = await ctx.run_db(ctx.db.get_rule_card_snapshot, int(rule_id)) if hasattr(ctx.db, "get_rule_card_snapshot") else None
    if not snapshot:
        return None, None
    text = user_ui.build_user_rule_card_text(snapshot)
    keyboard = user_ui.build_user_rule_card_keyboard(
        rule_id=int(rule_id),
        is_active=bool(snapshot.get("is_active")),
        schedule_mode=str(snapshot.get("schedule_mode") or "interval"),
        mode=str(snapshot.get("mode") or "repost"),
    )
    return text, keyboard


async def ensure_rule_callback_access(ctx: UserHandlersContext, callback: CallbackQuery, rule_id: int) -> bool:
    user_id = callback.from_user.id if callback.from_user else 0
    if ctx.is_admin_user(user_id):
        return True
    owned = await ctx.run_db(ctx.is_rule_owned_by_user, rule_id, user_id)
    if owned:
        return True
    ctx.logger.warning("попытка доступа к чужому правилу user_id=%s rule_id=%s", user_id, rule_id)
    await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому правилу", show_alert=True)
    return False


def register_user_rule_handlers(dp: Dispatcher, ctx: UserHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data == "user_rules")
    async def handle_user_rules_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        ctx.logger.info("пользователь открыл список правил user_id=%s tenant_id=%s", user_id, tenant_id)
        rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id) if hasattr(ctx.db, "get_rules_for_tenant") else []
        await ctx.answer_callback_safe_once(callback)
        text = (
            "⚙️ Мои правила\n\n"
            "У вас пока нет правил.\n\n"
            "Создайте первое правило:\n"
            "1. выберите источник;\n"
            "2. выберите получателя;\n"
            "3. настройте правило в карточке."
            if not rules
            else "⚙️ Мои правила"
        )
        kb = build_user_rules_keyboard(rules, page=0, rules_page_size=ctx.rules_page_size, compact_rule_text=ctx.compact_rule_text)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data == "user_rules_noop")
    async def handle_user_rules_noop(callback: CallbackQuery):
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rules_page:"))
    async def handle_user_rules_page(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        page = int((callback.data or "user_rules_page:0").split(":", 1)[1])
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, callback.from_user.id if callback.from_user else 0)
        rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id) if hasattr(ctx.db, "get_rules_for_tenant") else []
        await ctx.answer_callback_safe_once(callback)
        if not rules:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=(
                    "⚙️ Мои правила\n\n"
                    "У вас пока нет правил.\n\n"
                    "Создайте первое правило:\n"
                    "1. выберите источник;\n"
                    "2. выберите получателя;\n"
                    "3. настройте правило в карточке."
                ),
                reply_markup=build_user_rules_keyboard(rules, page=0, rules_page_size=ctx.rules_page_size, compact_rule_text=ctx.compact_rule_text),
            )
            return
        await ctx.edit_message_reply_markup_safe(
            message=callback.message,
            reply_markup=build_user_rules_keyboard(rules, page=page, rules_page_size=ctx.rules_page_size, compact_rule_text=ctx.compact_rule_text),
        )

    @dp.callback_query(lambda c: c.data == "user_rules_add")
    async def handle_user_rules_add(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        ctx.logger.info("пользователь начал создание правила user_id=%s tenant_id=%s", user_id, tenant_id)
        subscription = await ctx.run_db(ctx.subscription_service.get_active_subscription, tenant_id) or ctx.get_plan_info("FREE", "ru")
        usage_today = await ctx.run_db(ctx.usage_service.get_today_usage, tenant_id)
        rules_count = await ctx.run_db(ctx.db.count_rules_for_tenant, tenant_id) if hasattr(ctx.db, "count_rules_for_tenant") else 0
        can_create, reason = await ctx.run_db(ctx.limit_service.can_create_rule, tenant_id)
        if not can_create:
            if str(reason or "").strip().lower() == "подписка неактивна":
                await ctx.run_db(
                    ctx.write_billing_event,
                    tenant_id,
                    "subscription_blocked_action",
                    action="create_rule",
                    reason=reason,
                    plan_name=str(subscription.get("plan_name") or "FREE"),
                    usage_today=usage_today,
                )
                await ctx.answer_callback_safe_once(callback)
                await ctx.edit_message_text_safe(
                    message=callback.message,
                    text=user_ui.build_user_subscription_blocked_text(subscription),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="💎 Подписка", callback_data="user_subscription")],
                            [InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],
                        ]
                    ),
                )
                return
            await ctx.run_db(
                ctx.write_billing_event,
                tenant_id,
                "limit_rule_blocked",
                action="create_rule",
                reason=reason,
                plan_name=str(subscription.get("plan_name") or "FREE"),
                usage_today=usage_today,
            )
            await ctx.answer_callback_safe_once(callback)
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=user_ui.build_user_limit_exceeded_text(reason, subscription, usage_today, int(rules_count or 0)),
                reply_markup=ctx.public_usage_keyboard(),
            )
            return
        source_rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, "source") if hasattr(ctx.db, "get_channels_for_tenant") else []
        sources = [ctx.channel_choice_cls(r["channel_id"], r["thread_id"], r["title"] or r["channel_id"]) for r in source_rows]
        if not sources:
            await ctx.answer_callback_safe_once(callback)
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="Для создания правила сначала добавьте источник и получатель.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="📡 Добавить источник", callback_data="user_sources_add")],
                        [InlineKeyboardButton(text="🎯 Добавить получатель", callback_data="user_targets_add")],
                        [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_rules")],
                    ]
                ),
            )
            return
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=f"📤 {i}. {s.title}{f' (тема {s.thread_id})' if s.thread_id else ''}", callback_data=f"user_rule_pick_source:{i - 1}")]
                for i, s in enumerate(sources, 1)
            ]
            + [
                [InlineKeyboardButton(text="➕ Добавить источник", callback_data="user_sources_add")],
                [InlineKeyboardButton(text="⬅️ Назад в Мои правила", callback_data="user_rules")],
            ]
        )
        ctx.user_states[user_id] = {"action": "pick_rule_source", "sources": sources}
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="📤 Выберите источник\n\nИсточник — канал или тема группы, откуда ViMi возьмёт публикации.",
            reply_markup=keyboard,
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_open:"))
    async def handle_user_rule_open(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        ctx.logger.info("пользователь открыл карточку rule_id=%s", rule_id)
        text, kb = await build_user_rule_card_payload(ctx, rule_id)
        if not text or not kb:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_refresh:"))
    async def handle_user_rule_refresh(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        text, kb = await build_user_rule_card_payload(ctx, rule_id)
        if not text or not kb:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_extra:"))
    async def handle_user_rule_extra(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        snapshot = await ctx.run_db(ctx.db.get_rule_card_snapshot, int(rule_id)) if hasattr(ctx.db, "get_rule_card_snapshot") else None
        if not snapshot:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_rule_extra_text(
                rule_id=rule_id,
                target_title=str(snapshot.get("target_title") or snapshot.get("target_id") or "—"),
            ),
            reply_markup=user_ui.build_user_rule_extra_keyboard(rule_id=rule_id, mode=str(snapshot.get("mode") or "repost")),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_logs:"))
    async def handle_user_rule_logs(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        rows = await ctx.run_db(ctx.db.get_audit_for_rule, int(rule_id), 50) if hasattr(ctx.db, "get_audit_for_rule") else []
        rows = list(rows or [])
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_rule_logs_text(rule_id=rule_id, log_rows=rows),
            reply_markup=user_ui.build_user_rule_logs_keyboard(rule_id=rule_id, has_logs=bool(rows)),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_logs_refresh:"))
    async def handle_user_rule_logs_refresh(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        rows = await ctx.run_db(ctx.db.get_audit_for_rule, int(rule_id), 50) if hasattr(ctx.db, "get_audit_for_rule") else []
        rows = list(rows or [])
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_rule_logs_text(rule_id=rule_id, log_rows=rows),
            reply_markup=user_ui.build_user_rule_logs_keyboard(rule_id=rule_id, has_logs=bool(rows)),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions:"))
    async def handle_user_rule_reactions(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        settings = await ctx.run_db(ctx.db.get_rule_reaction_settings_for_tenant, tenant_id, rule_id) if hasattr(ctx.db, "get_rule_reaction_settings_for_tenant") else None
        accounts = await ctx.run_db(ctx.db.list_reaction_accounts_for_tenant, tenant_id, False) if hasattr(ctx.db, "list_reaction_accounts_for_tenant") else []
        status = "🟢 Включены" if bool(settings and settings.get("enabled")) else "⚪️ Выключены"
        mode = str((settings or {}).get("mode") or "premium_then_normal")
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=(
                f"⚙️ Реакции правила #{rule_id}\n\n"
                f"Статус: {status}\n"
                f"Режим: {mode}\n"
                f"Аккаунтов-реакторов: {len(accounts)}\n\n"
                "Подключите аккаунты вашей команды, чтобы они автоматически ставили реакции под публикациями ваших правил.\n\n"
                "🚧 Подключение аккаунтов и детальные настройки будут включены следующим обновлением."
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад в дополнительные функции", callback_data=f"user_rule_extra:{rule_id}")]]
            ),
        )
