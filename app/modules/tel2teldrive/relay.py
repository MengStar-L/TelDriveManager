from __future__ import annotations

import asyncio
import os
import re
import shutil
import uuid
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any

from app import database as db
from app.modules.aria2teldrive.teldrive_client import TelDriveClient


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
ACTIVE_STATUSES = {"pending", "downloading", "uploading", "cleaning"}


def make_relay_job_id(channel_id: int | None, message_id: int) -> str:
    channel = str(channel_id or "unknown").replace("-", "n")
    return f"tgrelay-{channel}-{int(message_id)}"


def sanitize_filename(name: str, fallback: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", str(name or "").strip())
    safe = safe.strip(" .")
    return safe or fallback


class TelegramRelayManager:
    def __init__(self, logger: Any, broker: Any):
        self.logger = logger
        self.broker = broker
        self.client: Any | None = None
        self.config: Any | None = None
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._semaphore: asyncio.Semaphore | None = None
        self._stopped = True

    async def start(self, client: Any, config: Any):
        if not getattr(config, "relay_enabled", False):
            await self.stop()
            return
        await db.init_db()
        self.client = client
        self.config = config
        self._stopped = False
        self._semaphore = asyncio.Semaphore(max(1, int(getattr(config, "relay_concurrency", 1) or 1)))
        active_jobs = await db.get_active_telegram_relay_jobs()
        for job in active_jobs:
            await self._schedule(job["job_id"])
        self.logger.info(f"Telegram 回源队列已启用，待处理任务 {len(active_jobs)} 个")

    async def stop(self):
        self._stopped = True
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.client = None
        self.config = None
        self._semaphore = None

    async def enqueue_message(self, client: Any, config: Any, msg: Any, file_info: dict[str, Any]) -> dict:
        await db.init_db()
        self.client = client
        self.config = config
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(max(1, int(getattr(config, "relay_concurrency", 1) or 1)))
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
            self.logger.info(f"已加入 Telegram 回源队列: {file_info['name']} (msg_id={message_id})")
        else:
            self.logger.warning(f"Telegram 回源任务已存在且处于终态，跳过重复入队: {job_id}")
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

    async def _schedule(self, job_id: str):
        if self._stopped or job_id in self._tasks:
            return
        task = asyncio.create_task(self._run_job(job_id))
        self._tasks[job_id] = task
        task.add_done_callback(lambda done, jid=job_id: self._tasks.pop(jid, None))

    async def _run_job(self, job_id: str):
        semaphore = self._semaphore or asyncio.Semaphore(1)
        async with semaphore:
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
                        self.logger.error(f"Telegram 回源任务失败: {job_id} - {exc}")
                        return
                    await db.update_telegram_relay_job(
                        job_id,
                        status="pending",
                        error=str(exc),
                        retry_count=retry_count,
                    )
                    await self._broadcast_job_id(job_id)
                    self.logger.warning(f"Telegram 回源任务准备重试: {job_id} ({retry_count}/{max_attempts}) - {exc}")
                    await asyncio.sleep(min(5 * retry_count, 30))

    async def _process_job(self, job: dict):
        client = self.client
        config = self.config
        if client is None or config is None:
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
        self.logger.info(f"Telegram 回源重传完成: {job.get('file_name')} (msg_id={source_message_id})")

    async def _download_message(self, client: Any, message: Any, path: Path, job: dict):
        loop = asyncio.get_running_loop()
        last_report = {"time": 0.0, "progress": -1.0}
        job_id = job["job_id"]

        def progress_callback(current: int, total: int):
            progress = round((current / total) * 100, 1) if total else 0.0
            now = loop.time()
            if progress < 100 and progress - last_report["progress"] < 1.0 and now - last_report["time"] < 1.0:
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

        async def progress_callback(uploaded: int, total: int, confirmed_parts: int, total_parts: int):
            if total_parts > 0:
                progress = round(min(confirmed_parts, total_parts) / total_parts * 100, 1)
            elif total > 0:
                progress = round(min(uploaded, total) / total * 100, 1)
            else:
                progress = 100.0
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
                    self.logger.warning(f"skip relay cleanup outside download dir: {resolved}")
                    return
            else:
                expected = str(job_id or "").strip()
                if not expected or (resolved.name != expected and resolved.parent.name != expected):
                    self.logger.warning(f"skip relay cleanup without active config: {resolved}")
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
            self.logger.warning(f"Telegram 回源本地文件清理失败: {local_path}, {exc}")
