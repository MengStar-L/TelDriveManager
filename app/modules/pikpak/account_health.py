"""Background PikPak account health checks."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app import database as db
from app.config import load_config, reload_config, save_config
from app.modules.pikpak.account_pool import PikPakAccountPool, pikpak_account_pool
from app.modules.pikpak.client import PikPakClient

logger = logging.getLogger(__name__)

DEFAULT_HEALTH_CHECK_URL = "https://mypikpak.com/s/VOveL7ZI01ViAz9VVKGgSWDlo2"
DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS = 6 * 60 * 60
DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS = 60


@dataclass
class AccountHealthProbeResult:
    restored_ids: list[str]
    cleanup_error: str = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _health_config() -> tuple[str, int, float]:
    pikpak_cfg = load_config().get("pikpak", {})
    url = str(pikpak_cfg.get("account_health_check_url") or DEFAULT_HEALTH_CHECK_URL).strip()
    try:
        interval = int(pikpak_cfg.get("account_health_check_interval") or DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS)
    except (TypeError, ValueError):
        interval = DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS
    try:
        timeout = float(pikpak_cfg.get("account_health_check_timeout") or DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS)
    except (TypeError, ValueError):
        timeout = DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS
    return url or DEFAULT_HEALTH_CHECK_URL, max(60, interval), max(5.0, timeout)


def health_next_check_iso(interval_seconds: int | None = None) -> str:
    if interval_seconds is None:
        _url, interval_seconds, _timeout = _health_config()
    return (datetime.now(timezone.utc) + timedelta(seconds=max(60, int(interval_seconds)))).isoformat(timespec="seconds")


def _first_share_file_id(files: list[dict[str, Any]]) -> str:
    for item in files or []:
        file_id = str(item.get("id") or item.get("file_id") or "").strip()
        if file_id:
            return file_id
    return ""


def _extract_share_id(share_link: str) -> str:
    match = re.search(r"/s/([^/?#]+)", str(share_link or "").strip())
    return match.group(1) if match else ""


def _restored_file_ids(result: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    direct_id = str(result.get("file_id") or "").strip()
    if direct_id:
        ids.append(direct_id)
    for item in result.get("task_info", []) or []:
        if not isinstance(item, dict):
            continue
        file_id = str(item.get("file_id") or "").strip()
        if file_id:
            ids.append(file_id)
    return list(dict.fromkeys(ids))


async def probe_account_by_transfer(client: PikPakClient, health_url: str) -> AccountHealthProbeResult:
    """Verify credentials by restoring one file from a known share, then deleting it."""

    share_id = _extract_share_id(health_url)
    if not share_id:
        raise ValueError("账号健康检查分享链接无效")

    share_info = await client.client.get_share_info(health_url, None)
    if isinstance(share_info, ValueError):
        raise share_info
    if not isinstance(share_info, dict):
        raise RuntimeError("账号健康检查分享链接返回异常")

    test_file_id = _first_share_file_id(share_info.get("files", []) or [])
    if not test_file_id:
        raise RuntimeError("账号健康检查分享链接没有可转存文件")

    pass_code_token = str(share_info.get("pass_code_token") or "")
    restore_result = await client.client.restore(share_id, pass_code_token, [test_file_id])
    restored_ids = _restored_file_ids(restore_result if isinstance(restore_result, dict) else {})
    if not restored_ids:
        raise RuntimeError("账号健康检查转存未返回文件 ID")

    cleanup_error = ""
    try:
        await client.delete_files(restored_ids)
    except Exception as exc:
        cleanup_error = str(exc)

    return AccountHealthProbeResult(restored_ids=restored_ids, cleanup_error=cleanup_error)


def _enabled_accounts_for_check() -> list[dict[str, Any]]:
    return [
        dict(account)
        for account in load_config(force_reload=True).get("pikpak", {}).get("accounts", [])
        if isinstance(account, dict) and bool(account.get("enabled", True))
    ]


def _due_accounts() -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    due: list[dict[str, Any]] = []
    for account in _enabled_accounts_for_check():
        next_at = parse_iso_datetime(account.get("health_next_check_at"))
        if next_at is None or next_at <= now:
            due.append(account)
    return due


def update_account_health_state(
    account_id: str,
    *,
    status: str,
    checked_at: str | None = None,
    next_check_at: str | None = None,
    error: str = "",
    session: str = "",
) -> dict[str, Any] | None:
    cfg = load_config(force_reload=True)
    pikpak_cfg = dict(cfg.get("pikpak", {}))
    accounts = [
        dict(account)
        for account in pikpak_cfg.get("accounts", [])
        if isinstance(account, dict)
    ]
    for index, account in enumerate(accounts):
        if str(account.get("id") or "").strip() != str(account_id or "").strip():
            continue
        account["health_status"] = status
        account["health_checked_at"] = checked_at or now_iso()
        account["health_next_check_at"] = next_check_at or health_next_check_iso()
        account["health_error"] = str(error or "").strip()
        account["updated_at"] = now_iso()
        if session:
            account["session"] = session
        accounts[index] = account
        pikpak_cfg["accounts"] = accounts
        save_config({"pikpak": pikpak_cfg})
        for saved in reload_config().get("pikpak", {}).get("accounts", []):
            if isinstance(saved, dict) and str(saved.get("id") or "").strip() == str(account_id or "").strip():
                return saved
        return account
    return None


class PikPakAccountHealthMonitor:
    def __init__(self, account_pool: PikPakAccountPool):
        self.account_pool = account_pool
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name="pikpak-account-health-monitor")

    async def stop(self) -> None:
        if not self._task:
            return
        self._stop_event.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.check_due_accounts()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("PikPak 账号健康检查循环异常")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    async def check_due_accounts(self) -> None:
        for account in _due_accounts():
            if self._stop_event.is_set():
                return
            await self.check_account(account)

    async def check_account(self, account: dict[str, Any]) -> None:
        account_id = str(account.get("id") or "").strip()
        account_name = str(account.get("name") or account.get("username") or account_id or "PikPak 账号")
        if not account_id:
            return

        health_url, interval_seconds, timeout_seconds = _health_config()
        checked_at = now_iso()
        next_check_at = health_next_check_iso(interval_seconds)

        try:
            result, client = await self._probe(account_id, health_url, timeout_seconds)
        except Exception as first_error:
            logger.warning("PikPak 账号健康检查失败，准备刷新后复测: account=%s, error=%s", account_name, first_error)
            try:
                await self.account_pool.close_account(account_id)
                result, client = await self._probe(account_id, health_url, timeout_seconds)
            except Exception as second_error:
                message = f"账号不可用：{second_error}"
                await db.add_pikpak_account_error(account_id, None, health_url, "account_health", message)
                update_account_health_state(
                    account_id,
                    status="failed",
                    checked_at=checked_at,
                    next_check_at=next_check_at,
                    error=message,
                )
                await self.account_pool.close_account(account_id)
                logger.error("PikPak 账号健康检查失败: account=%s, error=%s", account_name, second_error)
                return

        encoded_token = str(getattr(client.client, "encoded_token", "") or "")
        warning = f"测试文件清理失败：{result.cleanup_error}" if result.cleanup_error else ""
        update_account_health_state(
            account_id,
            status="available",
            checked_at=checked_at,
            next_check_at=next_check_at,
            error=warning,
            session=encoded_token,
        )
        if warning:
            logger.warning("PikPak 账号健康检查通过但清理失败: account=%s, error=%s", account_name, result.cleanup_error)
        else:
            logger.info("PikPak 账号健康检查通过: account=%s", account_name)

    async def _probe(
        self,
        account_id: str,
        health_url: str,
        timeout_seconds: float,
    ) -> tuple[AccountHealthProbeResult, PikPakClient]:
        _account, client = await self.account_pool.client_for_account(account_id)
        result = await asyncio.wait_for(probe_account_by_transfer(client, health_url), timeout=timeout_seconds)
        return result, client


account_health_monitor = PikPakAccountHealthMonitor(pikpak_account_pool)
