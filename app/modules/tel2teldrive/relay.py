from __future__ import annotations

import asyncio
import base64
import re
import shutil
import traceback
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError
from telethon.password import compute_check
from telethon.tl.functions.account import GetPasswordRequest
from telethon.tl.functions.auth import CheckPasswordRequest, ExportLoginTokenRequest, ImportLoginTokenRequest
from telethon.tl.types import auth

from app import database as db
from app.modules.aria2teldrive.teldrive_client import TelDriveClient


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
ACTIVE_STATUSES = {"pending", "downloading", "uploading", "cleaning"}
DEFAULT_RELAY_SESSION_NAME = "tel2teldrive_relay_session"
RELAY_LOG_LIMIT = 300


def make_relay_job_id(channel_id: int | None, message_id: int) -> str:
    channel = str(channel_id or "unknown").replace("-", "n")
    return f"tgrelay-{channel}-{int(message_id)}"


def sanitize_filename(name: str, fallback: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", str(name or "").strip())
    safe = safe.strip(" .")
    return safe or fallback


def build_relay_proxy(config: Any):
    if not getattr(config, "relay_proxy_host", ""):
        return None
    try:
        import socks
    except ModuleNotFoundError as exc:
        raise RuntimeError("PySocks is required for telegram relay proxy support") from exc
    username = getattr(config, "relay_proxy_username", "") or None
    password = getattr(config, "relay_proxy_password", "") or None
    proxy_type = str(getattr(config, "relay_proxy_type", "socks5") or "socks5").strip().lower()
    proxy_constant = socks.HTTP if proxy_type in ("http", "https") else socks.SOCKS5
    return (
        proxy_constant,
        getattr(config, "relay_proxy_host"),
        int(getattr(config, "relay_proxy_port", 1080) or 1080),
        True,
        username,
        password,
    )


class TelegramRelayManager:
    def __init__(self, logger: Any, broker: Any):
        self.logger = logger
        self.broker = broker
        self.client: TelegramClient | None = None
        self.config: Any | None = None
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._semaphore: asyncio.Semaphore | None = None
        self._stopped = True
        self._connect_lock = asyncio.Lock()
        self._login_task: asyncio.Task[Any] | None = None
        self._refresh_qr_event = asyncio.Event()
        self._password_future: asyncio.Future[str] | None = None
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

    async def start(self, _main_client: Any, config: Any):
        await self.apply_config(config)

    async def apply_config(self, config: Any):
        await db.init_db()
        self.config = config
        self._semaphore = asyncio.Semaphore(max(1, int(getattr(config, "relay_concurrency", 1) or 1)))
        enabled = bool(getattr(config, "relay_enabled", False))
        await self._update_state(
            enabled=enabled,
            session_name=self._session_name(config),
            concurrency=max(1, int(getattr(config, "relay_concurrency", 1) or 1)),
        )
        if not enabled:
            await self.stop()
            return
        self._stopped = False
        if not self._state.get("authorized"):
            await self._ensure_login_task()
        await self._schedule_active_jobs()

    async def stop(self):
        self._stopped = True
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._login_task and not self._login_task.done():
            self._login_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._login_task
        self._login_task = None
        self._refresh_qr_event.set()
        if self._password_future and not self._password_future.done():
            self._password_future.cancel()
        self._password_future = None
        if self.client is not None:
            with suppress(Exception):
                if self.client.is_connected():
                    await self.client.disconnect()
        self.client = None
        self.config = None
        self._semaphore = None
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
        await self.apply_config(config)

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
        if str(job.get("status")) not in {"failed", "cancelled"}:
            return {"success": False, "message": "only failed or cancelled relay jobs can be retried"}
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
        if not self.config or not getattr(self.config, "relay_enabled", False):
            raise RuntimeError("telegram relay is not enabled")
        self._refresh_qr_event.set()
        await self._ensure_login_task(force=True)

    async def submit_password(self, password: str):
        if not password:
            raise RuntimeError("password cannot be empty")
        if not self._password_future or self._password_future.done():
            raise RuntimeError("telegram relay is not waiting for password")
        self._password_future.set_result(password)

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

    async def _ensure_login_task(self, *, force: bool = False):
        if force and self._login_task and not self._login_task.done():
            self._login_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._login_task
            self._login_task = None
        if self._login_task and not self._login_task.done():
            return
        self._login_task = asyncio.create_task(self._login_loop())

    async def _login_loop(self):
        while not self._stopped and self.config is not None and getattr(self.config, "relay_enabled", False):
            try:
                client = await self._ensure_client()
                if await client.is_user_authorized():
                    await self._mark_authorized()
                    await self._schedule_active_jobs()
                    return
                await self._authorize_with_dashboard(client)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                tb = traceback.format_exc()
                with suppress(Exception):
                    self.logger.error(tb)
                self._log("ERROR", tb)
                self._log("ERROR", f"Relay login failed: {type(exc).__name__}: {exc}")
                await self._update_state(phase="error", authorized=False, last_error=str(exc))
                await asyncio.sleep(5)

    async def _ensure_client(self) -> TelegramClient:
        config = self.config
        if config is None:
            raise RuntimeError("telegram relay config is missing")
        async with self._connect_lock:
            session_name = self._session_name(config)
            if self.client is not None:
                if self.client.session.filename and Path(self.client.session.filename).stem != session_name:
                    with suppress(Exception):
                        await self.client.disconnect()
                    self.client = None
            if self.client is None:
                # Keep this identical to the proven main listener path: pass the
                # session name string directly and do not add relay-specific proxy
                # handling during login.
                self.client = TelegramClient(
                    session_name,
                    int(getattr(config, "telegram_api_id") or 0),
                    str(getattr(config, "telegram_api_hash") or ""),
                )
            if not self.client.is_connected():
                await self._update_state(phase="connecting", authorized=False, last_error=None)
                await self.client.connect()
            return self.client

    async def _authorize_with_dashboard(self, client: TelegramClient):
        config = self.config
        if config is None:
            raise RuntimeError("telegram relay config is missing")
        while not self._stopped and self.config is config and getattr(config, "relay_enabled", False):
            self._refresh_qr_event.clear()
            result = await client(
                ExportLoginTokenRequest(
                    api_id=int(getattr(config, "telegram_api_id") or 0),
                    api_hash=str(getattr(config, "telegram_api_hash") or ""),
                    except_ids=[],
                )
            )
            if await self._consume_login_result(client, result):
                return
            if not isinstance(result, auth.LoginToken):
                self._log("WARN", "Unexpected relay login token response, retrying")
                await asyncio.sleep(2)
                continue

            from app.modules.tel2teldrive.service import build_qr_data_uri, format_local_time

            token_b64 = base64.urlsafe_b64encode(result.token).decode("utf-8").rstrip("=")
            qr_image = build_qr_data_uri(f"tg://login?token={token_b64}")
            expires_at = result.expires.astimezone().isoformat(timespec="seconds")
            await self._update_state(
                phase="awaiting_qr",
                authorized=False,
                needs_password=False,
                qr_image=qr_image,
                qr_expires_at=expires_at,
                last_error=None,
            )
            self._log("INFO", f"Relay login QR generated, expires at {format_local_time(expires_at)}")

            while not self._stopped and self.config is config and getattr(config, "relay_enabled", False):
                if self._refresh_qr_event.is_set():
                    self._refresh_qr_event.clear()
                    self._log("INFO", "Relay login QR refresh requested")
                    break
                if datetime.now(timezone.utc) >= result.expires:
                    self._log("WARN", "Relay login QR expired, refreshing")
                    break
                await asyncio.sleep(3)
                try:
                    poll_result = await client(
                        ExportLoginTokenRequest(
                            api_id=int(getattr(config, "telegram_api_id") or 0),
                            api_hash=str(getattr(config, "telegram_api_hash") or ""),
                            except_ids=[],
                        )
                    )
                    if await self._consume_login_result(client, poll_result):
                        return
                except SessionPasswordNeededError:
                    await self._complete_password_login(client)
                    return
                except Exception as exc:
                    message = str(exc)
                    if "SESSION_PASSWORD_NEEDED" in message:
                        await self._complete_password_login(client)
                        return
                    if "TOKEN_EXPIRED" in message:
                        self._log("WARN", "Relay login token expired, refreshing")
                        break
                    raise

    async def _consume_login_result(self, client: TelegramClient, result: Any) -> bool:
        if isinstance(result, auth.LoginTokenSuccess):
            await self._mark_authorized()
            return True
        if isinstance(result, auth.LoginTokenMigrateTo):
            await client._switch_dc(result.dc_id)
            migrated = await client(ImportLoginTokenRequest(token=result.token))
            if isinstance(migrated, auth.LoginTokenSuccess):
                await self._mark_authorized()
                return True
        return False

    async def _mark_authorized(self):
        was_authorized = bool(self._state.get("authorized"))
        self._refresh_qr_event.clear()
        if self._password_future and not self._password_future.done():
            self._password_future.cancel()
        self._password_future = None
        await self._update_state(
            phase="authorized",
            authorized=True,
            needs_password=False,
            qr_image=None,
            qr_expires_at=None,
            last_error=None,
        )
        if not was_authorized:
            self._log("INFO", "Telegram relay login successful")

    async def _complete_password_login(self, client: TelegramClient):
        await self._update_state(
            phase="awaiting_password",
            authorized=False,
            needs_password=True,
            qr_image=None,
            qr_expires_at=None,
            last_error=None,
        )
        self._log("WARN", "Relay account requires 2FA password")
        while not self._stopped:
            loop = asyncio.get_running_loop()
            self._password_future = loop.create_future()
            try:
                password = await self._password_future
            except asyncio.CancelledError:
                return
            finally:
                self._password_future = None
            try:
                pwd = await client(GetPasswordRequest())
                await client(CheckPasswordRequest(password=compute_check(pwd, password)))
                await self._mark_authorized()
                return
            except PasswordHashInvalidError:
                self._log("ERROR", "Relay 2FA password is invalid")
                await self._update_state(
                    phase="awaiting_password",
                    authorized=False,
                    needs_password=True,
                    last_error="Relay 2FA password is invalid",
                )

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
                self._log("WARN", f"Relay account is not logged in; job remains pending: {job_id}")
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
        try:
            client = await self._ensure_client()
            if await client.is_user_authorized():
                await self._mark_authorized()
                return True
        except Exception as exc:
            self._log("ERROR", f"Relay authorization check failed: {exc}")
            await self._update_state(phase="error", authorized=False, last_error=str(exc))
            return False
        await self._ensure_login_task()
        return False

    async def _process_job(self, job: dict):
        client = await self._ensure_client()
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
        last_report = {"time": 0.0, "progress": -1.0}
        job_id = job["job_id"]

        def progress_callback(current: int, total: int):
            progress = round((current / total) * 100, 1) if total else 0.0
            now = loop.time()
            if progress < 100 and progress - last_report["progress"] < 2.0 and now - last_report["time"] < 1.5:
                return
            last_report["time"] = now
            last_report["progress"] = progress
            loop.create_task(self._update_download_progress(job_id, progress))

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
            parallel_chunk_upload=bool(config.upload_parallel_chunk_upload),
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

        return await teldrive.upload_file_chunked(
            str(path),
            str(config.teldrive_target_path or "/"),
            progress_callback,
        )

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

    async def _update_download_progress(self, job_id: str, progress: float):
        job = await db.get_telegram_relay_job(job_id)
        if not job or str(job.get("status")) != "downloading":
            return
        await db.update_telegram_relay_job(job_id, download_progress=progress)
        await self._broadcast_job_id(job_id)

    async def _broadcast_job_id(self, job_id: str):
        job = await db.get_telegram_relay_job(job_id)
        if job:
            await self._broadcast_job(job)

    async def _broadcast_job(self, job: dict):
        await self.broker._broadcast({"type": "relay_job_update", "payload": job})

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
