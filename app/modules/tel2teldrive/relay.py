from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import uuid
from collections import deque
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.types import InputChannel, InputPeerChannel
from telethon.utils import resolve_id

from app import database as db
from app.modules.aria2teldrive.teldrive_client import TelDriveClient


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
ACTIVE_STATUSES = {"pending", "downloading", "uploading", "cleaning"}
DEFAULT_RELAY_SESSION_NAME = "tel2teldrive_relay_session"
RELAY_LOG_LIMIT = 300
RELAY_WATCHDOG_INTERVAL = 20


def make_relay_job_id(channel_id: int | None, message_id: int) -> str:
    channel = str(channel_id or "unknown").replace("-", "n")
    return f"tgrelay-{channel}-{int(message_id)}"


def sanitize_filename(name: str, fallback: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", str(name or "").strip())
    safe = safe.strip(" .")
    return safe or fallback


# 多 bot 并行下载触发下限：小文件单连接已足够，省去多 bot 建连开销
MULTIBOT_MIN_SIZE = 8 * 1024 * 1024
# 切片粒度：work-stealing 队列里每个块的大小（bots 抢着下，天然均衡快慢 bot）
MULTIBOT_BLOCK_SIZE = 16 * 1024 * 1024
# 单 bot 单块 getFile 请求大小（须为 4KB 倍数且 ≤1MB）
MULTIBOT_REQUEST_SIZE = 512 * 1024
# 多 bot 下载用 os.pwrite 定位写盘（POSIX）；非 POSIX（如 Windows）回退单连接
_HAS_PWRITE = hasattr(os, "pwrite")


class BotDownloadPool:
    """回源专用的 bot 账号下载池（与主监听客户端完全解耦）。

    复用 TelDrive 在 teldrive.bots 里的 bot token——每个 bot 是独立账号、独立限速桶，
    并发拉取同一文件的不同字节区间后合并写盘，从而把单文件下载提到接近 N 倍。
    bot 用 token 登录、无需扫码；惰性连接（首次下载才建立连接），跨任务复用。
    """

    def __init__(self, api_id: int, api_hash: str, tokens: list[str], *,
                 proxy: Any = None, connections: int = 6, logger: Callable[[str, str], None] | None = None):
        self.api_id = int(api_id or 0)
        self.api_hash = str(api_hash or "")
        self.tokens = [t for t in (tokens or []) if t]
        self.proxy = proxy
        self.connections = max(1, int(connections or 1))
        self._log = logger or (lambda level, msg: None)
        self._clients: list[Any] = []
        self._cursor = 0           # 下一个待尝试的 token 下标（跳过坏 token，不回头）
        self._labels: dict[int, str] = {}   # id(client) -> 可读 bot 标识（@username 或 bot id）
        self._lock = asyncio.Lock()

    def signature(self) -> tuple:
        """用于判断配置是否变化、是否需要重建池。"""
        return (tuple(self.tokens), self.proxy, self.api_id, self.api_hash)

    def usable(self) -> bool:
        return _HAS_PWRITE and bool(self.tokens) and self.api_id > 0 and bool(self.api_hash)

    @staticmethod
    def _token_label(tok: str) -> str:
        """从 bot token 提取可读且不泄密的标识：冒号前的 bot 数字 id（token 私密部分不展示）。"""
        head = str(tok or "").split(":", 1)[0].strip()
        return f"bot#{head}" if head else "bot#?"

    def _label_of(self, client: Any) -> str:
        return self._labels.get(id(client), "bot#?")

    async def _acquire(self, n: int) -> list[Any]:
        """惰性连接，确保至多 n 个（仅受 token 总数约束）bot 在线，返回当前全部在线 bot。

        上限刻意只取 token 总数、而非 connections——这样在「部分 bot 不是源频道成员」时，
        调用方还能继续启用备用 bot 凑够下载并发数。真正并发下载的路数由调用方按 connections 截断。
        """
        async with self._lock:
            target = min(n, len(self.tokens))
            while len(self._clients) < target and self._cursor < len(self.tokens):
                tok = self.tokens[self._cursor]
                self._cursor += 1
                label = self._token_label(tok)
                try:
                    c = TelegramClient(StringSession(), self.api_id, self.api_hash, proxy=self.proxy)
                    await c.start(bot_token=tok)
                    with suppress(Exception):
                        me = await c.get_me()
                        uname = getattr(me, "username", None)
                        if uname:
                            label = f"@{uname}"
                    self._labels[id(c)] = label
                    self._clients.append(c)
                except Exception as exc:
                    self._log("WARN", f"回源 bot 启动失败已跳过 [{label}]: {type(exc).__name__}: {exc}")
            return list(self._clients)

    async def close(self):
        async with self._lock:
            for c in self._clients:
                with suppress(Exception):
                    await c.disconnect()
            self._clients = []
            self._labels = {}
            self._cursor = 0

    async def _resolve_doc(self, client: Any, real_id: int, msg_id: int) -> Any | None:
        """逐 bot 解析源频道实体并取出该消息的 document；失败返回 None 并记录是哪个 bot。

        bot 用空 session 登录，没有任何频道实体缓存。关键：不能直接拿 access_hash=0 调
        get_messages（→ channels.GetMessages → CHANNEL_INVALID）。须仿照 teldrive 两步式：
        先用 access_hash=0 调 channels.GetChannels 把频道“解析”出来——access_hash=0 只是
        “请按我的成员身份帮我查”的引导值，且仅 GetChannels 接受它，解析成功的前提是该 bot
        是源频道成员/管理员。拿到对自己账号有效的真实 access_hash 后，才能用它取消息。
        access_hash 与 file_reference 都按账号发放、跨账号失效，因此必须逐 bot 各取一份 document。
        """
        label = self._label_of(client)
        try:
            resolved = await client(GetChannelsRequest([InputChannel(real_id, 0)]))
            chats = getattr(resolved, "chats", None) or []
            if not chats:
                raise RuntimeError("频道未解析出（该 bot 可能不是源频道成员/管理员）")
            chat = chats[0]
            peer = InputPeerChannel(chat.id, chat.access_hash)
            m = await client.get_messages(peer, ids=msg_id)
            doc = getattr(m, "document", None) if m else None
            if doc is None:
                self._log("WARN", f"回源 bot 取下载信息失败，跳过 [{label}]: 源消息无可下载 document")
            return doc
        except Exception as exc:
            self._log("WARN", f"回源 bot 取下载信息失败，跳过 [{label}]: {type(exc).__name__}: {exc}")
            return None

    async def download(self, channel_id: int, msg_id: int, dest_path: str,
                       file_size: int, want: int, on_progress: Callable[[int, int], None] | None,
                       report_bots: Callable[[int], None] | None = None) -> bool:
        """用 bot 池并行下载单条消息的媒体到 dest_path。成功返回 True；失败/不可用返回 False（调用方回退单连接）。"""
        if file_size <= 0:
            return False  # 未知大小无法切片，交回退处理

        target = min(want, self.connections, len(self.tokens))
        if target <= 0:
            return False
        real_id, _ = resolve_id(int(channel_id))

        # 逐批启用 bot 并「并行」解析各自的源频道实体 + document：非源频道成员的 bot 会解析
        # 失败被跳过；只要还有备用 token，就继续启用，直到凑够 target 路或备用 bot 用尽。
        live: list[Any] = []
        docs: list[Any] = []
        tried: set[int] = set()
        while len(live) < target:
            # 需在线的连接数 = 已试过的 + 还差的，据此惰性启用恰好够用的新 bot。
            need = len(tried) + (target - len(live))
            clients = await self._acquire(need)
            new_clients = [c for c in clients if id(c) not in tried]
            if not new_clients:
                break  # 无备用 bot 可试了
            for c in new_clients:
                tried.add(id(c))
            results = await asyncio.gather(
                *[self._resolve_doc(c, real_id, msg_id) for c in new_clients]
            )
            for c, doc in zip(new_clients, results):
                if doc is not None:
                    live.append(c)
                    docs.append(doc)

        if not live:
            return False
        # 池可能跨任务复用，单批解析出的可用 bot 或多于目标——按 connections 截断，绝不超配。
        if len(live) > target:
            live = live[:target]
            docs = docs[:target]
        if len(live) < target:
            self._log(
                "WARN",
                f"回源可用 bot 不足：目标 {target} 路，实际仅 {len(live)} 路（备用 bot 已用尽）",
            )
        if report_bots:
            report_bots(len(live))

        # 预分配文件，便于各 worker 用 pwrite 定位写入
        with open(dest_path, "wb") as f:
            f.truncate(file_size)

        q: asyncio.Queue = asyncio.Queue()
        for off in range(0, file_size, MULTIBOT_BLOCK_SIZE):
            q.put_nowait((off, min(MULTIBOT_BLOCK_SIZE, file_size - off)))

        fd = os.open(dest_path, os.O_WRONLY)
        progress = {"done": 0}

        async def worker(client: Any, doc: Any):
            fails = 0
            while True:
                try:
                    off, length = q.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    pos = off
                    # iter_download 的 limit 单位是“块数”而非字节；offset/file_size 才是字节。
                    limit_chunks = (length + MULTIBOT_REQUEST_SIZE - 1) // MULTIBOT_REQUEST_SIZE
                    async for part in client.iter_download(
                        doc, offset=off, limit=limit_chunks,
                        request_size=MULTIBOT_REQUEST_SIZE, file_size=file_size,
                    ):
                        os.pwrite(fd, part, pos)
                        pos += len(part)
                        progress["done"] += len(part)
                        if on_progress:
                            on_progress(progress["done"], file_size)
                    fails = 0
                except Exception as exc:
                    q.put_nowait((off, length))     # 退回给其他 bot（FLOOD_WAIT/抖动）
                    fails += 1
                    self._log("WARN", f"回源 bot 分块下载失败，退回重试: {type(exc).__name__}: {exc}")
                    if fails >= 3:
                        return                       # 放弃这个 bot，让健康 bot 继续清空队列
                    await asyncio.sleep(1)

        try:
            await asyncio.gather(*[worker(c, d) for c, d in zip(live, docs)])
            return q.empty()
        finally:
            os.close(fd)


class TelegramRelayManager:
    """回源（relay）队列管理器。

    不再维护独立的 Telegram 客户端与第二次扫码登录：下载/删除源消息都复用主监听
    客户端（它已登录、且已缓存频道实体，能直接用裸频道 ID 调 get_messages/
    delete_messages）。本管理器退化为“纯下载 + 上传 TelDrive”的任务队列。
    主客户端在每次重连时会被重建，因此这里通过 ``bind_client_getter`` 注入一个
    取“当前在用主客户端”的回调，确保任务（含重载后的重试）始终拿到活的客户端。
    """

    def __init__(self, logger: Any, broker: Any):
        self.logger = logger
        self.broker = broker
        self.config: Any | None = None
        self._client_getter: Callable[[], Any] | None = None
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._semaphore: asyncio.Semaphore | None = None
        self._concurrency: int = 1
        self._bot_pool: BotDownloadPool | None = None
        self._stopped = True
        self._watchdog_task: asyncio.Task[Any] | None = None
        self._download_speed: dict[str, float] = {}
        self._download_bots: dict[str, int] = {}
        self._upload_workers: dict[str, int] = {}
        self._logs: deque[dict[str, Any]] = deque(maxlen=RELAY_LOG_LIMIT)
        self._state: dict[str, Any] = {
            "phase": "disabled",
            "enabled": False,
            "authorized": False,
            "needs_password": False,
            "qr_image": None,
            "qr_expires_at": None,
            "last_error": None,
            "session_name": DEFAULT_RELAY_SESSION_NAME,
            "updated_at": self._iso_now(),
        }

    def bind_client_getter(self, getter: Callable[[], Any]) -> None:
        """注入“取当前主监听客户端”的回调（由主服务在构造时绑定）。"""
        self._client_getter = getter

    def _current_client(self) -> Any | None:
        if self._client_getter is None:
            return None
        try:
            return self._client_getter()
        except Exception:
            return None

    async def start(self, _main_client: Any, config: Any):
        await self.apply_config(config)

    async def apply_config(self, config: Any):
        await db.init_db()
        self.config = config
        concurrency = max(1, int(getattr(config, "relay_concurrency", 1) or 1))
        if self._semaphore is None or concurrency != self._concurrency:
            self._semaphore = asyncio.Semaphore(concurrency)
            self._concurrency = concurrency
        enabled = bool(getattr(config, "relay_enabled", False))
        await self._update_state(
            enabled=enabled,
            session_name=self._session_name(config),
            concurrency=concurrency,
        )
        if not enabled:
            await self.stop()
            return
        self._stopped = False
        await self._refresh_bot_pool(config)
        await self._refresh_auth_state()
        self._ensure_watchdog()
        await self._schedule_active_jobs()

    async def _refresh_bot_pool(self, config: Any):
        """根据配置（惰性）准备回源 bot 下载池：仅更新 token/代理/连接数，真正下载时才建连。"""
        if not bool(getattr(config, "relay_multibot_enabled", False)):
            if self._bot_pool is not None:
                await self._bot_pool.close()
                self._bot_pool = None
            return
        from app.modules.tel2teldrive.service import (
            build_telegram_proxy,
            query_db_bot_tokens,
            run_blocking_io,
        )

        # token 来源：优先用户在前端自填的 bot_tokens（可移植，换服务器/频道也能用）；
        # 留空才回退读取 TelDrive 的 teldrive.bots。
        user_tokens = [t for t in (getattr(config, "relay_bot_tokens", []) or []) if str(t).strip()]
        if user_tokens:
            tokens = [str(t).strip() for t in user_tokens]
            token_source = "用户自填"
        else:
            tokens = await run_blocking_io(query_db_bot_tokens, config)
            token_source = "TelDrive 数据库"
        proxy = build_telegram_proxy(config)
        connections = max(1, int(getattr(config, "relay_download_connections", 6) or 6))
        api_id = int(getattr(config, "telegram_api_id", 0) or 0)
        api_hash = str(getattr(config, "telegram_api_hash", "") or "")
        new_sig = (tuple(tokens), proxy, api_id, api_hash)
        if self._bot_pool is not None and self._bot_pool.signature() == new_sig:
            self._bot_pool.connections = connections  # 仅连接数变化，热更新即可
            return
        if self._bot_pool is not None:
            await self._bot_pool.close()
        if not tokens:
            self._bot_pool = None
            self._log("WARN", "多 bot 下载已开启，但没有可用 bot token（自填为空且 teldrive.bots 也没有），回源将回退单连接下载")
            return
        self._bot_pool = BotDownloadPool(
            api_id, api_hash, tokens, proxy=proxy, connections=connections, logger=self._log
        )
        self._log(
            "INFO",
            f"回源多 bot 下载池就绪: {len(tokens)} 个 bot（来源: {token_source}），目标并发 {min(connections, len(tokens))}"
            f"{'，走代理' if proxy else '，直连'}",
        )

    async def stop(self):
        self._stopped = True
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._watchdog_task
        self._watchdog_task = None
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # 注意：绝不在此断开主监听客户端——它由主服务负责生命周期。
        if self._bot_pool is not None:
            await self._bot_pool.close()
            self._bot_pool = None
        await self._update_state(
            phase="disabled",
            enabled=False,
            authorized=False,
            needs_password=False,
            qr_image=None,
            qr_expires_at=None,
            last_error=None,
        )

    async def enqueue_message(self, _main_client: Any, config: Any, msg: Any, file_info: dict[str, Any]) -> dict:
        self.config = config
        if self._semaphore is None:
            self._concurrency = max(1, int(getattr(config, "relay_concurrency", 1) or 1))
            self._semaphore = asyncio.Semaphore(self._concurrency)
        self._stopped = False

        message_id = int(getattr(msg, "id"))
        channel_id = int(getattr(config, "telegram_channel_id") or 0)
        job_id = make_relay_job_id(channel_id, message_id)
        local_path = str(self._build_local_file_path(config, job_id, file_info["name"]))
        job = await db.add_telegram_relay_job(
            job_id,
            source_channel_id=channel_id,
            source_message_id=message_id,
            file_name=file_info["name"],
            file_size=int(file_info.get("size") or 0),
            mime_type=file_info.get("mime_type", ""),
            local_path=local_path,
        )
        await self._broadcast_job(job)
        if str(job.get("status") or "pending") in ACTIVE_STATUSES:
            await self._schedule(job_id)
            self._log("INFO", f"Queued Telegram relay job: {file_info['name']} (msg_id={message_id})")
        else:
            self._log("WARN", f"Relay job already terminal, skip duplicate enqueue: {job_id}")
        return job

    async def retry_job(self, job_id: str) -> dict:
        await db.init_db()
        job = await db.get_telegram_relay_job(job_id)
        if not job:
            return {"success": False, "message": "relay job not found"}
        if str(job.get("status")) == "completed":
            return {"success": False, "message": "completed relay jobs cannot be retried"}
        # 允许对失败/取消，以及卡住的进行中任务（下载/上传/清理/等待）手动重试：
        # 先取消可能在跑的任务，避免与重试竞争，再重置状态重新入队。
        task = self._tasks.pop(job_id, None)
        if task:
            task.cancel()
            with suppress(Exception):
                await task
        await db.update_telegram_relay_job(
            job_id,
            status="pending",
            error=None,
            download_progress=0.0,
            upload_progress=0.0,
            retry_count=0,
            completed_at=None,
        )
        await self._broadcast_job_id(job_id)
        await self._schedule(job_id)
        return {"success": True, "data": await db.get_telegram_relay_job(job_id)}

    async def cancel_job(self, job_id: str) -> dict:
        await db.init_db()
        job = await db.get_telegram_relay_job(job_id)
        if not job:
            return {"success": False, "message": "relay job not found"}
        if str(job.get("status")) in TERMINAL_STATUSES:
            return {"success": False, "message": "relay job already finished"}
        await db.update_telegram_relay_job(job_id, status="cancelled", error=None)
        task = self._tasks.get(job_id)
        if task:
            task.cancel()
        await self._broadcast_job_id(job_id)
        return {"success": True, "data": await db.get_telegram_relay_job(job_id)}

    async def delete_job(self, job_id: str) -> dict:
        await db.init_db()
        job = await db.get_telegram_relay_job(job_id)
        if not job:
            return {"success": False, "message": "relay job not found"}
        if str(job.get("status")) not in TERMINAL_STATUSES:
            return {"success": False, "message": "relay job is still active"}
        await self._cleanup_local_path(str(job.get("local_path") or ""), job_id=job_id)
        deleted = await db.delete_telegram_relay_job(job_id)
        if deleted:
            await self.broker._broadcast({"type": "relay_job_deleted", "payload": {"job_id": job_id}})
        return {"success": deleted}

    async def request_qr_refresh(self):
        raise RuntimeError("回源已复用主账号，无需单独登录")

    async def submit_password(self, _password: str):
        raise RuntimeError("回源已复用主账号，无需单独登录")

    def state_snapshot(self) -> dict[str, Any]:
        return dict(self._state)

    def logs_snapshot(self, limit: int = 200) -> list[dict[str, Any]]:
        data = list(self._logs)
        return data[-max(1, int(limit or 200)):]

    async def _schedule_active_jobs(self):
        active_jobs = await db.get_active_telegram_relay_jobs()
        scheduled_count = 0
        for job in active_jobs:
            if await self._schedule(job["job_id"]):
                scheduled_count += 1
        if scheduled_count:
            self._log("INFO", f"Scheduled {scheduled_count} active relay job(s)")

    async def _refresh_auth_state(self):
        """根据主监听客户端的登录状态刷新回源状态（仅在状态变化时广播，避免刷屏）。"""
        authorized = await self._is_authorized()
        phase = "authorized" if authorized else "waiting_main"
        if bool(self._state.get("authorized")) == authorized and self._state.get("phase") == phase:
            return
        await self._update_state(
            phase=phase,
            authorized=authorized,
            needs_password=False,
            qr_image=None,
            qr_expires_at=None,
            last_error=None if authorized else self._state.get("last_error"),
        )

    def _ensure_watchdog(self):
        if self._watchdog_task and not self._watchdog_task.done():
            return
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    async def _watchdog_loop(self):
        # 主账号可能在任务入队后才登录/重连。定期巡检：主账号就绪即补调度待处理任务，
        # 避免任务因某次鉴权未就绪而永久卡在“等待中”。
        while not self._stopped and self.config is not None and getattr(self.config, "relay_enabled", False):
            try:
                await asyncio.sleep(RELAY_WATCHDOG_INTERVAL)
            except asyncio.CancelledError:
                raise
            if self._stopped:
                break
            await self._refresh_auth_state()
            if await self._is_authorized():
                await self._schedule_active_jobs()

    async def _schedule(self, job_id: str):
        if self._stopped or job_id in self._tasks:
            return False
        task = asyncio.create_task(self._run_job(job_id))
        self._tasks[job_id] = task
        task.add_done_callback(lambda done, jid=job_id: self._tasks.pop(jid, None))
        return True

    async def _run_job(self, job_id: str):
        semaphore = self._semaphore or asyncio.Semaphore(1)
        async with semaphore:
            if not await self._is_authorized():
                self._log("WARN", f"主账号未登录，回源任务等待中: {job_id}")
                return
            max_attempts = max(1, int(getattr(self.config, "relay_max_retries", 1) or 1))
            while not self._stopped:
                job = await db.get_telegram_relay_job(job_id)
                if not job or str(job.get("status")) in TERMINAL_STATUSES:
                    return
                try:
                    await self._process_job(job)
                    return
                except asyncio.CancelledError:
                    if not self._stopped:
                        await db.update_telegram_relay_job(job_id, status="cancelled")
                        await self._broadcast_job_id(job_id)
                    return
                except Exception as exc:
                    latest = await db.get_telegram_relay_job(job_id) or job
                    retry_count = int(latest.get("retry_count") or 0) + 1
                    if retry_count >= max_attempts:
                        await db.update_telegram_relay_job(
                            job_id,
                            status="failed",
                            error=str(exc),
                            retry_count=retry_count,
                        )
                        await self._broadcast_job_id(job_id)
                        self._log("ERROR", f"Relay job failed: {job_id} - {exc}")
                        return
                    await db.update_telegram_relay_job(
                        job_id,
                        status="pending",
                        error=str(exc),
                        retry_count=retry_count,
                    )
                    await self._broadcast_job_id(job_id)
                    self._log("WARN", f"Relay job will retry: {job_id} ({retry_count}/{max_attempts}) - {exc}")
                    await asyncio.sleep(min(5 * retry_count, 30))

    async def _is_authorized(self) -> bool:
        client = self._current_client()
        if client is None:
            return False
        try:
            if not client.is_connected():
                return False
            return bool(await client.is_user_authorized())
        except Exception as exc:
            self._log("ERROR", f"Relay authorization check failed: {exc}")
            return False

    async def _process_job(self, job: dict):
        client = self._current_client()
        if client is None:
            raise RuntimeError("主 Telegram 客户端不可用")
        config = self.config
        if config is None:
            raise RuntimeError("telegram relay manager is not started")

        job_id = job["job_id"]
        local_path = str(job.get("local_path") or "")
        if not local_path:
            local_path = str(self._build_local_file_path(config, job_id, job["file_name"]))
            await db.update_telegram_relay_job(job_id, local_path=local_path)
        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        expected_size = int(job.get("file_size") or 0)
        if not path.exists() or (expected_size > 0 and path.stat().st_size != expected_size):
            if path.exists():
                with suppress(Exception):
                    path.unlink()
            await db.update_telegram_relay_job(job_id, status="downloading", error=None, download_progress=0.0)
            await self._broadcast_job_id(job_id)
            message = await client.get_messages(int(job["source_channel_id"]), ids=int(job["source_message_id"]))
            if message is None:
                raise RuntimeError("source Telegram message is missing")
            await self._download_message(client, message, path, job)
        await db.update_telegram_relay_job(job_id, status="uploading", download_progress=100.0, upload_progress=0.0)
        await self._broadcast_job_id(job_id)

        result = await self._upload_local_file(path, config, job)
        if not result.get("success"):
            raise RuntimeError(result.get("error") or "upload failed")
        file_id = await self._record_teldrive_mapping(config, job, result)
        upload_id = str((result.get("upload_meta") or {}).get("upload_id") or "")
        await db.update_telegram_relay_job(
            job_id,
            status="cleaning",
            upload_progress=100.0,
            teldrive_file_id=file_id or "",
            upload_id=upload_id,
            upload_confirmed_parts_json="[]",
            upload_remote_parts_json="[]",
        )
        await self._broadcast_job_id(job_id)

        from app.modules.tel2teldrive.service import remember_internal_deleted_message_ids

        source_message_id = int(job["source_message_id"])
        remember_internal_deleted_message_ids([source_message_id])
        await client.delete_messages(int(job["source_channel_id"]), [source_message_id])
        await self._cleanup_local_path(str(path), job_id=job_id)
        await db.update_telegram_relay_job(
            job_id,
            status="completed",
            error=None,
            completed_at=datetime.now().astimezone().isoformat(timespec="seconds"),
        )
        await self._broadcast_job_id(job_id)
        self._log("INFO", f"Relay reupload completed: {job.get('file_name')} (msg_id={source_message_id})")

    async def _download_message(self, client: Any, message: Any, path: Path, job: dict):
        loop = asyncio.get_running_loop()
        last_report = {"time": loop.time(), "progress": -1.0, "bytes": 0}
        job_id = job["job_id"]
        file_size = int(job.get("file_size") or 0)

        def progress_callback(current: int, total: int):
            total = total or file_size
            progress = round((current / total) * 100, 1) if total else 0.0
            now = loop.time()
            if progress < 100 and progress - last_report["progress"] < 2.0 and now - last_report["time"] < 1.5:
                return
            dt = now - last_report["time"]
            speed = (current - last_report["bytes"]) / dt if dt > 0 else 0.0
            last_report["time"] = now
            last_report["progress"] = progress
            last_report["bytes"] = current
            loop.create_task(self._update_download_progress(job_id, progress, max(0.0, speed)))

        # 优先走 bot 池并行下载（大文件）；不可用/失败则回退主客户端单连接下载
        used_pool = False
        pool = self._bot_pool
        if pool is not None and pool.usable() and file_size >= MULTIBOT_MIN_SIZE:
            want = max(1, int(getattr(self.config, "relay_download_connections", 6) or 6))

            def report_bots(n: int):
                self._download_bots[job_id] = int(n)

            try:
                used_pool = await pool.download(
                    int(job["source_channel_id"]),
                    int(job["source_message_id"]),
                    str(path),
                    file_size,
                    want,
                    progress_callback,
                    report_bots,
                )
                if not used_pool:
                    self._log("WARN", f"多 bot 下载未完成，回退单连接: {job.get('file_name')}")
            except Exception as exc:
                self._log("WARN", f"多 bot 下载异常，回退单连接: {type(exc).__name__}: {exc}")
                used_pool = False
            finally:
                self._download_bots.pop(job_id, None)

        if not used_pool:
            downloaded = await client.download_media(message, file=str(path), progress_callback=progress_callback)
            final_path = Path(downloaded) if downloaded else path
            if final_path != path and final_path.exists():
                if path.exists():
                    path.unlink()
                final_path.replace(path)

        if not path.exists():
            raise RuntimeError("Telegram download did not create local file")
        await db.update_telegram_relay_job(job_id, download_progress=100.0, local_path=str(path))
        await self._broadcast_job_id(job_id)

    @staticmethod
    def _calc_upload_fingerprint(path: Path, chunk_size: int) -> str:
        """上传源指纹：chunk_size + 文件大小 + mtime。任一变化即判续传 checkpoint 失效
        （chunk_size 变 → partNo 边界错位；文件变 → 内容错位），续传前丢弃重传。
        mtime 用纳秒（st_mtime_ns），避免同秒内同尺寸内容替换被整秒截断成相同指纹。"""
        try:
            stat = path.stat()
        except OSError:
            return ""
        return f"chunk={int(chunk_size or 0)}|size={stat.st_size}|mtime={stat.st_mtime_ns}"

    @staticmethod
    def _load_json_list(raw: Any) -> list:
        if isinstance(raw, (list, tuple)):
            return list(raw)
        try:
            data = json.loads(raw) if raw else []
        except (TypeError, ValueError):
            return []
        return data if isinstance(data, list) else []

    async def _upload_local_file(self, path: Path, config: Any, job: dict) -> dict:
        teldrive = TelDriveClient(
            api_host=config.teldrive_url,
            access_token=config.bearer_token,
            channel_id=int(config.teldrive_channel_id or 0),
            chunk_size=config.teldrive_chunk_size,
            upload_concurrency=max(1, int(config.teldrive_upload_concurrency or 1)),
            random_chunk_name=bool(config.teldrive_random_chunk_name),
            max_retries=max(1, int(config.upload_max_retries or 1)),
            min_throughput_kbps=max(16, int(config.upload_min_throughput_kbps or 100)),
            # 并行上传已稳定，回源路径同样默认常开，不再受配置开关控制。
            parallel_chunk_upload=True,
        )
        job_id = job["job_id"]
        last_report = {"time": 0.0, "progress": -1.0}

        async def progress_callback(uploaded: int, total: int, confirmed_parts: int, total_parts: int):
            if total_parts > 0:
                progress = round(min(confirmed_parts, total_parts) / total_parts * 100, 1)
            elif total > 0:
                progress = round(min(uploaded, total) / total * 100, 1)
            else:
                progress = 100.0
            now = asyncio.get_running_loop().time()
            if progress < 100 and progress - last_report["progress"] < 2.0 and now - last_report["time"] < 1.5:
                return
            last_report["time"] = now
            last_report["progress"] = progress
            await db.update_telegram_relay_job(job_id, upload_progress=progress)
            await self._broadcast_job_id(job_id)

        # 与正常文件上传一致：上报“当前并行上传分块数”，>1 时前端卡片展示一致的“N 路并行”徽标。
        # 仅在并行上传（upload.parallel_chunk_upload）开启且多块文件时，TelDriveClient 才会回调 >1。
        def report_workers(active: int):
            try:
                n = int(active or 0)
            except (TypeError, ValueError):
                n = 0
            if n > 1:
                self._upload_workers[job_id] = n
            else:
                self._upload_workers.pop(job_id, None)

        # 断点续传：复用上次未完成上传的 upload_id + 已确认分块，避免每次失败整文件重传
        # （回源常失败的根因）。源指纹（chunk_size+大小+mtime）变化即判 checkpoint 失效。
        current_fp = self._calc_upload_fingerprint(path, teldrive.chunk_size)
        stored_fp = str(job.get("upload_source_fingerprint") or "")
        persisted_upload_id = str(job.get("upload_id") or "").strip()
        resumable = bool(persisted_upload_id and current_fp and stored_fp == current_fp)
        if resumable:
            upload_id = persisted_upload_id
            confirmed_part_numbers = self._load_json_list(job.get("upload_confirmed_parts_json"))
            remote_parts = self._load_json_list(job.get("upload_remote_parts_json"))
            if confirmed_part_numbers or remote_parts:
                self._log(
                    "INFO",
                    f"回源上传续传: {job.get('file_name')} 复用 {len(confirmed_part_numbers)} 个已确认分块",
                )
        else:
            # 指纹变了（chunk_size/本地文件变更）→ 丢弃服务端旧会话，换新 upload_id，避免错位旧块混入
            if persisted_upload_id and stored_fp and stored_fp != current_fp:
                with suppress(Exception):
                    await teldrive.cleanup_upload_session(persisted_upload_id)
            upload_id = str(uuid.uuid4())
            confirmed_part_numbers = []
            remote_parts = []
            await db.update_telegram_relay_job(
                job_id,
                upload_id=upload_id,
                upload_source_fingerprint=current_fp,
                upload_confirmed_parts_json="[]",
                upload_remote_parts_json="[]",
            )

        async def part_confirm_callback(part_no, part, confirmed_numbers, confirmed_remote_parts, total_parts):
            # 每确认一个分块就落库（best-effort）；落库失败只是退化为不续传，绝不连累本次上传
            try:
                await db.update_telegram_relay_job(
                    job_id,
                    upload_id=upload_id,
                    upload_confirmed_parts_json=json.dumps(confirmed_numbers, ensure_ascii=False),
                    upload_remote_parts_json=json.dumps(confirmed_remote_parts, ensure_ascii=False),
                )
            except Exception as exc:
                self._log("WARN", f"回源上传 checkpoint 落库失败（不影响本次上传）: {type(exc).__name__}: {exc}")

        try:
            return await teldrive.upload_file_chunked(
                str(path),
                str(config.teldrive_target_path or "/"),
                progress_callback,
                upload_id=upload_id,
                confirmed_part_numbers=confirmed_part_numbers,
                remote_parts=remote_parts,
                part_confirm_callback=part_confirm_callback,
                concurrency_callback=report_workers,
            )
        finally:
            self._upload_workers.pop(job_id, None)

    async def _record_teldrive_mapping(self, config: Any, job: dict, result: dict) -> str:
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        file_id = str(data.get("id") or "").strip()
        if not file_id:
            return ""
        message_ids = self._extract_uploaded_message_ids(result)
        if not message_ids:
            return file_id

        from app.modules.tel2teldrive.service import (
            load_mapping,
            merge_message_ids,
            record_teldrive_action,
            run_blocking_io,
            save_mapping,
        )

        mapping = await run_blocking_io(load_mapping)
        mapping[file_id] = merge_message_ids(mapping.get(file_id), message_ids)
        await run_blocking_io(save_mapping, mapping)
        await record_teldrive_action(
            config,
            action="auto_add",
            file_id=file_id,
            file_name=str(job.get("file_name") or file_id),
            reason="telegram_relay_reupload",
            message_ids=message_ids,
            file_size=int(job.get("file_size") or 0),
        )
        return file_id

    @staticmethod
    def _extract_uploaded_message_ids(result: dict) -> list[int]:
        candidates = []
        if isinstance(result.get("remote_parts"), list):
            candidates.extend(result["remote_parts"])
        upload_meta = result.get("upload_meta") if isinstance(result.get("upload_meta"), dict) else {}
        if isinstance(upload_meta.get("remote_parts"), list):
            candidates.extend(upload_meta["remote_parts"])
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        if isinstance(data.get("parts"), list):
            candidates.extend(data["parts"])
        ids: list[int] = []
        seen: set[int] = set()
        for part in candidates:
            if not isinstance(part, dict):
                continue
            raw = part.get("partId", part.get("id"))
            try:
                message_id = int(str(raw))
            except (TypeError, ValueError):
                continue
            if message_id <= 0 or message_id in seen:
                continue
            seen.add(message_id)
            ids.append(message_id)
        return ids

    async def _update_download_progress(self, job_id: str, progress: float, speed: float = 0.0):
        job = await db.get_telegram_relay_job(job_id)
        if not job or str(job.get("status")) != "downloading":
            return
        self._download_speed[job_id] = speed
        await db.update_telegram_relay_job(job_id, download_progress=progress)
        await self._broadcast_job_id(job_id)

    async def _broadcast_job_id(self, job_id: str):
        job = await db.get_telegram_relay_job(job_id)
        if job:
            await self._broadcast_job(job)

    async def _broadcast_job(self, job: dict):
        # download_speed 为运行时瞬时值（不落库），仅下载中携带，其余状态归零并清理。
        payload = dict(job)
        job_id = payload.get("job_id")
        status = str(payload.get("status"))
        if status == "downloading":
            payload["download_speed"] = self._download_speed.get(job_id, 0.0)
            payload["download_bots"] = self._download_bots.get(job_id, 0)
        else:
            self._download_speed.pop(job_id, None)
            self._download_bots.pop(job_id, None)
            payload["download_speed"] = 0.0
            payload["download_bots"] = 0
        # upload_workers 同为运行时瞬时值（不落库），仅上传中携带，其余状态归零并清理。
        if status == "uploading":
            payload["upload_workers"] = self._upload_workers.get(job_id, 0)
        else:
            self._upload_workers.pop(job_id, None)
            payload["upload_workers"] = 0
        await self.broker._broadcast({"type": "relay_job_update", "payload": payload})

    async def _update_state(self, **kwargs: Any):
        self._state.update(kwargs)
        self._state["updated_at"] = self._iso_now()
        await self.broker._broadcast({"type": "relay_state", "payload": self.state_snapshot()})

    def _log(self, level: str, message: str):
        entry = {
            "id": str(len(self._logs) + 1),
            "timestamp": self._iso_now(),
            "level": level,
            "message": message,
        }
        self._logs.append(entry)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self.broker._broadcast({"type": "relay_log", "payload": entry}))

    def _download_root(self, config: Any | None = None) -> Path:
        cfg = config or self.config
        raw = str(getattr(cfg, "relay_download_dir", "./telegram_relay") or "./telegram_relay")
        root = Path(raw)
        if not root.is_absolute():
            from app.modules.tel2teldrive.service import CONFIG_PATH

            root = CONFIG_PATH.parent / root
        root.mkdir(parents=True, exist_ok=True)
        return root.resolve()

    def _build_local_file_path(self, config: Any, job_id: str, file_name: str) -> Path:
        safe_name = sanitize_filename(file_name, f"{job_id}.bin")
        return self._download_root(config) / job_id / safe_name

    async def _cleanup_local_path(self, local_path: str, *, job_id: str | None = None):
        if not local_path:
            return
        path = Path(local_path)
        try:
            resolved = path.resolve()
            root = self._download_root() if self.config is not None else None
            cleanup_empty_parent = True
            if root is not None:
                if root not in resolved.parents and resolved != root:
                    self._log("WARN", f"skip relay cleanup outside download dir: {resolved}")
                    return
            else:
                expected = str(job_id or "").strip()
                if not expected or (resolved.name != expected and resolved.parent.name != expected):
                    self._log("WARN", f"skip relay cleanup without active config: {resolved}")
                    return
                cleanup_empty_parent = resolved.parent.name == expected
            if resolved.is_dir():
                shutil.rmtree(resolved)
            elif resolved.exists():
                resolved.unlink()
            parent = resolved.parent
            if cleanup_empty_parent and parent.exists() and (root is None or parent != root) and not any(parent.iterdir()):
                parent.rmdir()
        except Exception as exc:
            self._log("WARN", f"Relay local cleanup failed: {local_path}, {exc}")

    @staticmethod
    def _session_name(config: Any | None) -> str:
        value = str(getattr(config, "relay_session_name", "") or "").strip()
        return value or DEFAULT_RELAY_SESSION_NAME

    @staticmethod
    def _iso_now() -> str:
        return datetime.now().astimezone().isoformat(timespec="seconds")
