from __future__ import annotations

from pathlib import Path
import re

from telethon import TelegramClient
from telethon.errors import PasswordHashInvalidError, PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError

from app.reaction_service import ReactionService


class ReactionAuthService:
    """Auth-сервис для подключения reaction-аккаунтов только через защищённый web onboarding flow.

    Важно: нельзя использовать этот сервис для ввода Telegram login-code/2FA через Telegram-бота.
    """

    def __init__(self, db, *, api_id: int, api_hash: str):
        self.db = db
        self.api_id = api_id
        self.api_hash = api_hash
        self.reaction_service = ReactionService(db)

    def normalize_phone(self, phone: str) -> str:
        value = str(phone or "").strip()
        if not re.fullmatch(r"\+[0-9]{8,16}", value):
            raise ValueError("Некорректный формат номера. Используйте формат: +79991234567")
        return value

    def mask_phone(self, phone: str) -> str:
        normalized = self.normalize_phone(phone)
        digits = normalized[1:]
        if len(digits) <= 7:
            return f"+{digits[:2]}***{digits[-2:]}"
        return f"+{digits[:4]}***{digits[-4:]}"

    def build_tmp_session_path(self, tenant_id: int, user_id: int, rule_id: int) -> Path:
        session_dir = self.reaction_service.get_session_dir_for_tenant(tenant_id)
        return session_dir / f"tmp_rule_{rule_id}_user_{user_id}.session"

    def build_final_session_name(self, telegram_user_id: int) -> str:
        return f"tenant_reactor_{telegram_user_id}"

    def build_final_session_path(self, tenant_id: int, telegram_user_id: int) -> Path:
        session_dir = self.reaction_service.get_session_dir_for_tenant(tenant_id)
        return session_dir / f"{self.build_final_session_name(telegram_user_id)}.session"

    def create_client(self, session_path: Path) -> TelegramClient:
        session_base = session_path.with_suffix("") if session_path.suffix == ".session" else session_path
        return TelegramClient(
            str(session_base),
            self.api_id,
            self.api_hash,
            connection_retries=8,
            retry_delay=2,
            request_retries=8,
            auto_reconnect=True,
        )

    async def start_phone_login(self, *, tenant_id: int, rule_id: int, user_id: int, phone: str) -> dict:
        normalized_phone = self.normalize_phone(phone)
        tmp_session_path = self.build_tmp_session_path(tenant_id, user_id, rule_id)
        client = self.create_client(tmp_session_path)
        try:
            await client.connect()
            if await client.is_user_authorized():
                await client.disconnect()
                self.cleanup_tmp_session(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id)
                client = self.create_client(tmp_session_path)
                await client.connect()
            sent = await client.send_code_request(normalized_phone)
            return {
                "phone": normalized_phone,
                "phone_hint": self.mask_phone(normalized_phone),
                "phone_code_hash": sent.phone_code_hash,
                "session_base": str(tmp_session_path.with_suffix("")),
                "requires_code": True,
            }
        finally:
            await client.disconnect()

    async def complete_code_login(self, *, tenant_id: int, rule_id: int, user_id: int, phone: str, phone_code_hash: str, code: str) -> dict:
        tmp_session_path = self.build_tmp_session_path(tenant_id, user_id, rule_id)
        client = self.create_client(tmp_session_path)
        try:
            await client.connect()
            try:
                await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
            except SessionPasswordNeededError:
                return {"status": "password_required"}
            except PhoneCodeInvalidError:
                raise ValueError("Неверный код. Проверьте код и попробуйте снова.")
            except PhoneCodeExpiredError:
                self.cleanup_tmp_session(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id)
                raise ValueError("Срок действия кода истёк. Начните подключение заново.")
            return await self.finalize_authorized_client(
                tenant_id=tenant_id,
                rule_id=rule_id,
                user_id=user_id,
                client=client,
                phone=phone,
            )
        finally:
            await client.disconnect()

    async def complete_password_login(self, *, tenant_id: int, rule_id: int, user_id: int, password: str, phone: str) -> dict:
        tmp_session_path = self.build_tmp_session_path(tenant_id, user_id, rule_id)
        client = self.create_client(tmp_session_path)
        try:
            await client.connect()
            try:
                await client.sign_in(password=password)
            except PasswordHashInvalidError:
                raise ValueError("Неверный 2FA-пароль. Проверьте пароль и попробуйте снова.")
            return await self.finalize_authorized_client(
                tenant_id=tenant_id,
                rule_id=rule_id,
                user_id=user_id,
                client=client,
                phone=phone,
            )
        finally:
            await client.disconnect()

    async def finalize_authorized_client(self, *, tenant_id: int, rule_id: int, user_id: int, client: TelegramClient, phone: str) -> dict:
        me = await client.get_me()
        telegram_user_id = int(me.id)
        username = getattr(me, "username", None)
        is_premium = bool(getattr(me, "premium", False))
        tmp_session_path = self.build_tmp_session_path(tenant_id, user_id, rule_id)
        final_session_name = self.build_final_session_name(telegram_user_id)
        final_session_path = self.build_final_session_path(tenant_id, telegram_user_id)
        if final_session_path.exists() and tmp_session_path.exists() and final_session_path != tmp_session_path:
            tmp_session_path.unlink(missing_ok=True)
        elif tmp_session_path.exists() and final_session_path != tmp_session_path:
            final_session_path.unlink(missing_ok=True)
            tmp_session_path.rename(final_session_path)
        self.cleanup_tmp_session(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id)
        account_id = self.db.create_reaction_account(
            tenant_id=tenant_id,
            session_name=final_session_name,
            telegram_user_id=telegram_user_id,
            username=username,
            phone_hint=self.mask_phone(phone),
            is_premium=is_premium,
            fixed_reactions=[],
            status="active",
        )
        return {
            "status": "success",
            "account_id": account_id,
            "telegram_user_id": telegram_user_id,
            "username": username,
            "is_premium": is_premium,
            "phone_hint": self.mask_phone(phone),
        }

    def cleanup_tmp_session(self, *, tenant_id: int, rule_id: int, user_id: int) -> None:
        base = self.build_tmp_session_path(tenant_id, user_id, rule_id)
        for suffix in ("", "-journal", "-wal", "-shm"):
            (base.parent / f"{base.name}{suffix}").unlink(missing_ok=True)
