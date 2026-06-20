"""PikPak account pool with round-robin client selection."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from app.config import load_config
from app.modules.pikpak.client import PikPakClient


@dataclass(frozen=True)
class PikPakAccountContext:
    id: str
    name: str
    login_mode: str
    username: str = ""

    @classmethod
    def from_config(cls, account: dict[str, Any]) -> "PikPakAccountContext":
        return cls(
            id=str(account.get("id") or "").strip(),
            name=str(account.get("name") or account.get("username") or "PikPak 账号").strip(),
            login_mode=str(account.get("login_mode") or "password").strip().lower(),
            username=str(account.get("username") or "").strip(),
        )


class PikPakAccountPool:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._clients: dict[str, PikPakClient] = {}
        self._cursor = 0

    def enabled_accounts(self) -> list[dict[str, Any]]:
        cfg = load_config()
        accounts = cfg.get("pikpak", {}).get("accounts", [])
        return [
            account for account in accounts
            if isinstance(account, dict) and bool(account.get("enabled", True))
        ]

    async def next_client(self) -> tuple[PikPakAccountContext, PikPakClient]:
        async with self._lock:
            accounts = self.enabled_accounts()
            if not accounts:
                raise RuntimeError("请先添加并启用至少一个 PikPak 账号")
            if self._cursor >= len(accounts):
                self._cursor = 0
            account = accounts[self._cursor]
            self._cursor = (self._cursor + 1) % len(accounts)
            return PikPakAccountContext.from_config(account), await self._get_or_create_client(account)

    async def client_for_account(self, account_id: str) -> tuple[PikPakAccountContext, PikPakClient]:
        normalized_id = str(account_id or "").strip()
        async with self._lock:
            for account in load_config().get("pikpak", {}).get("accounts", []):
                if isinstance(account, dict) and str(account.get("id") or "").strip() == normalized_id:
                    return PikPakAccountContext.from_config(account), await self._get_or_create_client(account)
        raise KeyError(f"PikPak 账号不存在: {normalized_id}")

    async def _get_or_create_client(self, account: dict[str, Any]) -> PikPakClient:
        account_id = str(account.get("id") or "").strip()
        client = self._clients.get(account_id)
        if client is not None:
            return client
        client = PikPakClient(
            username=str(account.get("username") or ""),
            password=str(account.get("password") or ""),
            session=str(account.get("session") or ""),
            login_mode=str(account.get("login_mode") or "password"),
            save_dir=str(load_config().get("pikpak", {}).get("save_dir") or "/"),
        )
        await client.login()
        self._clients[account_id] = client
        return client

    async def reset(self):
        async with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
            self._cursor = 0
        for client in clients:
            await client.close()

    async def close_account(self, account_id: str):
        normalized_id = str(account_id or "").strip()
        async with self._lock:
            client = self._clients.pop(normalized_id, None)
            self._cursor = 0
        if client is not None:
            await client.close()


pikpak_account_pool = PikPakAccountPool()
