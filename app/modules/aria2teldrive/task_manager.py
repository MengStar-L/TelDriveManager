"""任务管理器 - 监控 aria2 下载并自动上传到 TelDrive"""

import asyncio
import json
import os
import shutil
import logging
import psutil
import uuid
from typing import Optional, Set
from pathlib import Path

from app.config import load_config
from app.aria2_client import Aria2Client
from app.modules.aria2teldrive.teldrive_client import TelDriveClient
from app import database as db


def get_aria2_rpc_url(config: dict) -> str:

    """获取 Aria2 RPC 地址"""
    return config.get("aria2", {}).get("rpc_url", "http://localhost:6800/jsonrpc")


def get_download_dir(config: dict) -> str:

    """获取下载目录"""
    download_dir = config.get("aria2", {}).get("download_dir", "./downloads")
    path = Path(download_dir)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent.parent.parent.parent / path
    path.mkdir(parents=True, exist_ok=True)
    return str(path.resolve())


logger = logging.getLogger(__name__)


class TaskManager:
    """任务管理器 - 监控 aria2 并自动上传"""

    def __init__(self):
        self.config = load_config()

        self.aria2: Optional[Aria2Client] = None
        self.teldrive: Optional[TelDriveClient] = None
        self._ws_clients: Set = set()
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False
        # 内存缓存：已知的 GID 集合，避免重复查库
        self._known_gids: set = set()
        # 终态 GID 集合：已完成/失败/取消的任务，不再查库更新
        self._terminal_gids: set = set()
        # 正在上传的 GID 集合，避免重复触发上传
        self._uploading_gids: set = set()
        # 上传并发控制：用活跃计数+Event 实现动态并发，支持热更新
        self._active_uploads: int = 0
        self._upload_slot_event = asyncio.Event()
        self._upload_slot_event.set()  # 初始有空位
        # 上传协程追踪：task_id -> asyncio.Task，重试时可取消旧任务
        self._upload_tasks: dict = {}
        # 运行期任务扩展字段（不落库）：chunk 进度 / 重试提示等
        self._runtime_task_state: dict = {}
        # 上传重试计数：task_id -> 已重试次数

        self._upload_retry_counts: dict = {}
        self._upload_retry_checkpoints: dict = {}
        self._upload_confirmed_checkpoints: dict = {}
        self._upload_session_state: dict = {}
        self._upload_session_meta: dict = {}
        self._session_owner = f"{os.getpid()}:{uuid.uuid4().hex[:8]}"
        # 自动重试计时器
        self._last_retry_time: float = 0.0
        # 上传速度跟踪：per-task 已上传字节 → monitor loop 汇总算总速度
        self._task_uploaded_bytes: dict = {}   # task_id -> 当前已上传字节
        self._upload_total_snapshot: int = 0   # 上次快照时的总字节
        self._upload_time_snapshot: float = 0.0
        self._upload_speed: float = 0.0
        self._disk_usage_info: dict = {}  # 缓存磁盘使用信息
        self._cpu_info: dict = {}
        self._last_download_speed: int = 0  # 缓存最近的 aria2 下载速度
        self._disk_protection_active: bool = False
        self._disk_protection_applied_max_downloads: int = self._get_user_max_concurrent_downloads()
        self._disk_protection_info: dict = {
            "active": False,
            "message": "",
            "threshold_bytes": self._get_disk_protection_threshold_bytes(),
            "resume_threshold_bytes": self._get_disk_protection_resume_bytes(),
            "configured_max_concurrent": self._get_user_max_concurrent_downloads(),
            "applied_max_concurrent": self._get_user_max_concurrent_downloads(),
        }
        # 串行模式：被暂停的 aria2 GID 集合
        self._serial_gate_paused_gids: Set[str] = set()
        self._serial_gate_releasing_gids: Set[str] = set()
        self._serial_dispatch_lock = asyncio.Lock()



    def _init_clients(self):
        """根据当前配置初始化客户端"""
        cfg = self.config
        self.aria2 = Aria2Client(
            rpc_url=cfg["aria2"]["rpc_url"],
            rpc_port=cfg["aria2"]["rpc_port"],
            rpc_secret=cfg["aria2"]["rpc_secret"]
        )
        self.teldrive = TelDriveClient(
            api_host=cfg["teldrive"]["api_host"],
            access_token=cfg["teldrive"]["access_token"],
            channel_id=cfg["teldrive"]["channel_id"],
            chunk_size=cfg["teldrive"]["chunk_size"],
            upload_concurrency=self._get_effective_upload_concurrency(),
            random_chunk_name=cfg["teldrive"].get("random_chunk_name", True),
            max_retries=cfg.get("upload", {}).get("max_retries", 3)
        )

    def _require_aria2(self) -> Aria2Client:
        if self.aria2 is None:
            raise RuntimeError("aria2 client is not initialized")
        return self.aria2

    def _require_teldrive(self) -> TelDriveClient:
        if self.teldrive is None:
            raise RuntimeError("TelDrive client is not initialized")
        return self.teldrive

    async def _close_clients(self):
        old_aria2 = self.aria2
        self.aria2 = None
        if old_aria2 is not None:
            await old_aria2.close()

    async def reload_config(self):
        """重新加载配置并重建客户端"""
        await self._close_clients()
        self.config = load_config(force_reload=True)
        self._init_clients()

        # upload_concurrency 变更后无需重建对象，

        # _wait_upload_slot 每次实时读取 config 值
        # 唤醒等待槽位的协程，让它们用新并发数重新检查
        self._upload_slot_event.set()
        # 异步同步 aria2 全局选项
        asyncio.create_task(self._apply_aria2_options())


    def _get_upload_path(self, local_path: str) -> str:
        """将 aria2 下载路径映射到用户配置的上传文件目录。

        当用户设置了 upload_dir 时，用 upload_dir 替换 download_dir 前缀。
        local_path 已在 _sync_aria2_tasks 中用 item['dir'] + filename 构造，
        所以这里只需做简单的前缀替换。
        """
        upload_dir = self.config["teldrive"].get("upload_dir", "").strip()
        if not upload_dir:
            return local_path

        download_dir = self.config["aria2"].get("download_dir", "./downloads")
        norm_dl = os.path.normpath(download_dir)
        norm_fp = os.path.normpath(local_path)

        # 前缀替换 download_dir → upload_dir
        if norm_fp.startswith(norm_dl + os.sep) or norm_fp == norm_dl:
            rel = os.path.relpath(norm_fp, norm_dl)
            mapped = os.path.join(upload_dir, rel)
            logger.info(f"[路径映射] {local_path} -> {mapped}")
            return mapped

        # download_dir 不匹配时，直接用 upload_dir + 文件名
        filename = os.path.basename(norm_fp)
        mapped = os.path.join(upload_dir, filename)
        logger.info(f"[路径映射-文件名] {local_path} -> {mapped}")
        return mapped

    def _is_serial_transfer_mode_enabled(self) -> bool:
        return bool(self.config.get("upload", {}).get("serial_transfer_mode", False))

    def _get_user_max_concurrent_downloads(self) -> int:
        configured = max(1, int(self.config.get("aria2", {}).get("max_concurrent") or 3))
        return 1 if self._is_serial_transfer_mode_enabled() else configured

    def _get_effective_upload_concurrency(self) -> int:
        configured = max(1, int(self.config.get("teldrive", {}).get("upload_concurrency") or 4))
        return 1 if self._is_serial_transfer_mode_enabled() else configured

    def _get_disk_protection_threshold_gb(self) -> int:
        return max(1, int(self.config.get("aria2", {}).get("disk_protection_threshold_gb") or 5))

    def _get_disk_protection_threshold_bytes(self) -> int:
        return self._get_disk_protection_threshold_gb() * 1024 ** 3

    def _get_disk_protection_resume_bytes(self) -> int:
        return (self._get_disk_protection_threshold_gb() + 1) * 1024 ** 3

    def _is_disk_protection_enabled(self) -> bool:
        return not self._is_serial_transfer_mode_enabled()

    def _has_active_upload_work(self) -> bool:
        return bool(self._upload_tasks or self._uploading_gids or self._active_uploads > 0)

    def _is_disk_ready_for_serial_resume(self) -> bool:
        if self._is_serial_transfer_mode_enabled():
            return True
        if self._disk_protection_active:
            return False
        disk_info = self._disk_usage_info or {}
        if "free" not in disk_info:
            return False
        free_bytes = max(0, int(disk_info.get("free") or 0))
        return free_bytes >= self._get_disk_protection_resume_bytes()

    def _is_serial_gate_held(self, gid: str) -> bool:
        return bool(gid and gid in self._serial_gate_paused_gids)

    def _visible_aria2_status(self, aria2_status: str, gid: str) -> str:
        if gid in self._serial_gate_releasing_gids and aria2_status == "paused":
            return "pending"
        if aria2_status != "paused":
            self._serial_gate_releasing_gids.discard(gid)

        if (
            self._is_serial_transfer_mode_enabled()
            and self._is_serial_gate_held(gid)
            and aria2_status in ("active", "waiting", "paused")
        ):
            return "pending"

        status_map = {
            "active": "downloading",
            "waiting": "pending",
            "paused": "paused",
            "complete": "uploading",
            "error": "failed",
            "removed": "cancelled",
        }
        return status_map.get(aria2_status, "pending")

    @staticmethod
    def _serialize_aria2_options(options: Optional[dict]) -> str:
        clean = dict(options or {})
        clean.pop("pause", None)
        return json.dumps(clean, ensure_ascii=False)

    @staticmethod
    def _deserialize_aria2_options(raw_options) -> dict:
        if isinstance(raw_options, dict):
            options = dict(raw_options)
        else:
            try:
                options = json.loads(raw_options or "{}")
            except Exception:
                options = {}
        if not isinstance(options, dict):
            options = {}
        options.pop("pause", None)
        return options

    def _prepare_aria2_options(self, task: dict, fallback_options: Optional[dict] = None) -> dict:
        options = self._deserialize_aria2_options(task.get("aria2_options_json"))
        if fallback_options:
            options.update(self._deserialize_aria2_options(fallback_options))
        if not options.get("dir"):
            options["dir"] = self.config.get("aria2", {}).get("download_dir", "./downloads")
        filename = task.get("filename")
        if filename and not options.get("out"):
            options["out"] = filename
        options.pop("pause", None)
        return options

    def _parse_aria2_item(self, item: dict) -> dict:
        parsed = Aria2Client.parse_status(item)
        task_dir = item.get("dir", "")
        bt_name = item.get("bittorrent", {}).get("info", {}).get("name", "")
        if bt_name and task_dir:
            parsed["file_path"] = os.path.join(task_dir, bt_name)
            parsed["filename"] = bt_name
        elif task_dir and parsed.get("filename"):
            parsed["file_path"] = os.path.join(task_dir, parsed["filename"])
        return parsed

    def _is_managed_local_path(self, local_path: str) -> bool:
        if not local_path:
            return False
        try:
            target = Path(local_path).resolve()
        except Exception:
            return False

        bases = []
        try:
            bases.append(Path(get_download_dir(self.config)).resolve())
        except Exception:
            pass

        upload_dir = self.config.get("teldrive", {}).get("upload_dir", "").strip()
        if upload_dir:
            try:
                bases.append(Path(upload_dir).resolve())
            except Exception:
                pass

        for base in bases:
            try:
                target.relative_to(base)
            except ValueError:
                continue
            return target != base
        return False

    def _delete_managed_path(self, local_path: str) -> bool:
        if not local_path or not os.path.exists(local_path):
            return False
        if not self._is_managed_local_path(local_path):
            logger.warning(f"skip deleting unmanaged queued download path: {local_path}")
            return False
        try:
            if os.path.isdir(local_path):
                shutil.rmtree(local_path)
            else:
                os.remove(local_path)
            logger.info(f"deleted queued aria2 residue: {local_path}")
            return True
        except Exception as e:
            logger.warning(f"failed to delete queued aria2 residue: {local_path}, {e}")
            return False

    def _queued_local_candidates(self, task: dict, parsed: Optional[dict] = None) -> list[str]:
        options = self._prepare_aria2_options(task)
        candidates = []
        for path in (
            (parsed or {}).get("file_path"),
            task.get("local_path"),
        ):
            if path:
                candidates.append(path)

        if options.get("dir") and options.get("out"):
            candidates.append(os.path.join(options["dir"], options["out"]))

        expanded = []
        seen = set()
        for path in candidates:
            for candidate in (path, f"{path}.aria2"):
                if candidate and candidate not in seen:
                    seen.add(candidate)
                    expanded.append(candidate)
        return expanded

    async def _cleanup_queued_aria2_files(self, task: dict, parsed: Optional[dict] = None) -> bool:
        cleaned = False
        for local_path in self._queued_local_candidates(task, parsed):
            cleaned = self._delete_managed_path(local_path) or cleaned
        if cleaned:
            await self._check_disk_usage()
        return cleaned

    async def _has_db_download_in_flight(self) -> bool:
        for task in await db.get_all_tasks():
            if task.get("status") == "downloading" and task.get("aria2_gid"):
                return True
        return False

    @staticmethod
    def _is_live_serial_aria2_status(status: str) -> bool:
        return str(status or "") in ("active", "waiting", "paused")

    async def _has_live_serial_download_slot(
        self,
        active: Optional[list] = None,
        waiting: Optional[list] = None,
        stopped: Optional[list] = None,
    ) -> bool:
        if not self._is_serial_transfer_mode_enabled() or not self.aria2:
            return False

        item_by_gid = {
            item.get("gid"): item
            for item in (active or []) + (waiting or []) + (stopped or [])
            if item.get("gid")
        }
        aria2 = self._require_aria2()

        for task in await db.get_all_tasks():
            gid = str(task.get("aria2_gid") or "")
            status = str(task.get("status") or "")
            if not gid or status not in ("downloading", "pending"):
                continue

            item = item_by_gid.get(gid)
            if item is None:
                try:
                    item = await aria2.tell_status(gid)
                except Exception:
                    item = {}

            parsed = self._parse_aria2_item(item) if item else {}
            if self._is_live_serial_aria2_status(parsed.get("status")):
                return True

        return False

    async def enqueue_serial_task(self, url: str, filename: Optional[str] = None,
                                  teldrive_path: str = "/", aria2_options: Optional[dict] = None,
                                  task_id: Optional[str] = None) -> dict:
        normalized_path = self._normalize_teldrive_path(teldrive_path)
        options = self._deserialize_aria2_options(aria2_options)
        if filename and not options.get("out"):
            options["out"] = filename
        if not options.get("dir"):
            options["dir"] = self.config.get("aria2", {}).get("download_dir", "./downloads")

        task_id = task_id or f"queued-{uuid.uuid4().hex}"
        options_json = self._serialize_aria2_options(options)
        await db.add_task(task_id, url, filename, normalized_path, options_json)
        await db.update_task(
            task_id,
            status="pending",
            aria2_gid=None,
            aria2_options_json=options_json,
            teldrive_path=normalized_path,
            download_progress=0.0,
            upload_progress=0.0,
            download_speed="",
            upload_speed="",
            file_size="",
            error=None,
            local_path=None,
        )
        self._clear_runtime_task_fields(task_id)
        await self._broadcast_task_update(task_id)
        task = self._merge_runtime_task_fields(await db.get_task(task_id))
        return task or {"task_id": task_id, "status": "pending"}

    async def _normalize_serial_pending_aria2_tasks(
        self, active: Optional[list] = None, waiting: Optional[list] = None,
        stopped: Optional[list] = None
    ) -> set[str]:
        if not self._is_serial_transfer_mode_enabled() or not self.aria2:
            return set()

        item_by_gid = {
            item.get("gid"): item
            for item in (active or []) + (waiting or []) + (stopped or [])
            if item.get("gid")
        }
        removed_gids: set[str] = set()
        aria2 = self._require_aria2()

        for task in await db.get_all_tasks():
            gid = str(task.get("aria2_gid") or "")
            if not gid or task.get("status") != "pending":
                continue

            item = item_by_gid.get(gid)
            if item is None:
                try:
                    item = await aria2.tell_status(gid)
                except Exception:
                    item = {}

            parsed = self._parse_aria2_item(item) if item else {}
            if self._is_live_serial_aria2_status(parsed.get("status")):
                if parsed.get("status") == "paused":
                    self._serial_gate_paused_gids.add(gid)
                continue
            if parsed.get("status") == "complete":
                continue

            try:
                await aria2.force_remove(gid)
            except Exception:
                try:
                    await aria2.remove(gid)
                except Exception as e:
                    logger.debug(f"failed to remove queued aria2 gid {gid}: {e}")

            await self._cleanup_queued_aria2_files(task, parsed)
            await db.update_task(
                task["task_id"],
                status="pending",
                aria2_gid=None,
                local_path=None,
                download_progress=0.0,
                download_speed="",
                file_size="",
                error=None,
            )
            self._known_gids.discard(gid)
            self._terminal_gids.add(gid)
            self._serial_gate_paused_gids.discard(gid)
            self._serial_gate_releasing_gids.discard(gid)
            removed_gids.add(gid)
            await self._broadcast_task_update(task["task_id"])

        return removed_gids

    async def _dispatch_next_serial_download(
        self, active: Optional[list] = None, waiting: Optional[list] = None,
        stopped: Optional[list] = None
    ) -> bool:
        if not self._is_serial_transfer_mode_enabled() or not self.aria2:
            return False
        if self._serial_dispatch_lock.locked():
            return False

        async with self._serial_dispatch_lock:
            aria2 = self._require_aria2()
            try:
                if active is None:
                    active = await aria2.tell_active() or []
                if waiting is None:
                    waiting = await aria2.tell_waiting(0, 1000) or []
                if stopped is None:
                    stopped = await aria2.tell_stopped_all() or []
            except Exception as e:
                logger.debug(f"serial dispatcher cannot inspect aria2: {e}")
                return False

            if any(item.get("gid") for item in (active or []) + (waiting or [])):
                return False
            if await self._has_db_download_in_flight():
                return False
            if await self._has_live_serial_download_slot(active, waiting, stopped):
                return False
            if await self._has_serial_resume_blockers(stopped):
                return False
            if not self._is_disk_ready_for_serial_resume():
                return False

            task = await db.get_next_pending_queued_task()
            if not task:
                return False

            url = str(task.get("url") or "").strip()
            if not url:
                await db.update_task(task["task_id"], status="failed", error="missing download URL")
                await self._broadcast_task_update(task["task_id"])
                return False

            options = self._prepare_aria2_options(task)
            try:
                gid = await aria2.add_uri(url, options)
            except Exception as e:
                logger.warning(f"serial dispatcher failed to add aria2 task {task['task_id']}: {e}")
                await db.update_task(task["task_id"], status="pending", download_speed="", error=str(e))
                await self._broadcast_task_update(task["task_id"])
                return False

            await db.update_task(
                task["task_id"],
                status="downloading",
                aria2_gid=gid,
                aria2_options_json=self._serialize_aria2_options(options),
                download_progress=0.0,
                upload_progress=0.0,
                download_speed="",
                upload_speed="",
                file_size="",
                error=None,
                local_path=None,
            )
            self._known_gids.add(gid)
            self._terminal_gids.discard(gid)
            self._serial_gate_paused_gids.discard(gid)
            self._serial_gate_releasing_gids.discard(gid)
            await self._broadcast_task_update(task["task_id"])
            logger.info(f"serial dispatcher released task {task['task_id']} to aria2 gid={gid}")
            return True

    async def _has_serial_resume_blockers(self, stopped: Optional[list] = None) -> bool:
        if self._has_active_upload_work():
            return True

        for item in stopped or []:
            gid = item.get("gid", "")
            if not gid or gid in self._terminal_gids:
                continue
            parsed = Aria2Client.parse_status(item)
            if parsed.get("status") != "complete":
                continue
            task = await db.get_task_by_gid(gid)
            if not task:
                task = await db.get_task(gid)
            if not task:
                return True
            if task.get("status") == "uploading":
                return True
            if task.get("status") not in ("completed", "cancelled", "failed"):
                return True

        all_tasks = await db.get_all_tasks()
        auto_delete = self.config.get("upload", {}).get("auto_delete", True)
        for task in all_tasks:
            if not self._is_upload_stage_task(task):
                continue
            status = str(task.get("status") or "")
            if status == "uploading":
                return True
            local_path = self._get_upload_path(task.get("local_path", ""))
            if status == "completed":
                if auto_delete and local_path and os.path.exists(local_path):
                    return True
                continue
            if status not in ("failed", "paused"):
                continue

            if local_path and os.path.exists(local_path):
                return True

        return False

    async def _pause_for_serial_gate(self, item: dict) -> bool:
        gid = item.get("gid", "")
        aria2 = self.aria2
        if not gid or not aria2:
            return False

        status = item.get("status")
        if status == "paused":
            self._serial_gate_paused_gids.add(gid)
            return True
        if status not in ("active", "waiting"):
            return False

        try:
            await aria2.force_pause(gid)
        except Exception:
            try:
                await aria2.pause(gid)
            except Exception as e:
                logger.debug(f"serial gate pause failed: {gid}, {e}")
                return False

        self._serial_gate_paused_gids.add(gid)
        return True

    async def _unpause_from_serial_gate(self, gid: str) -> bool:
        aria2 = self.aria2
        if not gid or not aria2:
            return False
        try:
            await aria2.unpause(gid)
        except Exception as e:
            logger.debug(f"serial gate unpause failed: {gid}, {e}")
            return False
        self._serial_gate_paused_gids.discard(gid)
        self._serial_gate_releasing_gids.add(gid)
        return True

    async def _sync_serial_transfer_gate_impl(
        self, active_items: list, queued_items: list, stopped: Optional[list] = None
    ):
        if not self._is_serial_transfer_mode_enabled():
            if not self._serial_gate_paused_gids:
                return
            paused_gids = set(self._serial_gate_paused_gids)
            for item in queued_items:
                gid = item.get("gid", "")
                if gid in paused_gids and item.get("status") == "paused":
                    await self._unpause_from_serial_gate(gid)
            return

        should_block_downloads = await self._has_serial_resume_blockers(stopped)
        if should_block_downloads:
            for item in active_items + queued_items:
                if item.get("status") in ("active", "waiting"):
                    await self._pause_for_serial_gate(item)
            return

        gated_active = [item for item in active_items if self._is_serial_gate_held(item.get("gid", ""))]
        if gated_active:
            for item in gated_active:
                await self._pause_for_serial_gate(item)
            return

        ungated_active = [item for item in active_items if not self._is_serial_gate_held(item.get("gid", ""))]
        if ungated_active:
            allowed_gid = ungated_active[0].get("gid", "")
            for item in active_items + queued_items:
                gid = item.get("gid", "")
                if gid == allowed_gid or item.get("status") not in ("active", "waiting"):
                    continue
                await self._pause_for_serial_gate(item)
            return

        gated_paused = [
            item for item in queued_items
            if item.get("status") == "paused" and self._is_serial_gate_held(item.get("gid", ""))
        ]
        if gated_paused:
            await self._unpause_from_serial_gate(gated_paused[0].get("gid", ""))
            return

        ungated_waiting = [
            item for item in queued_items
            if item.get("status") == "waiting" and not self._is_serial_gate_held(item.get("gid", ""))
        ]
        if ungated_waiting:
            for item in ungated_waiting[1:]:
                await self._pause_for_serial_gate(item)
            return

        return

    async def _sync_serial_transfer_gate(self, active: list, waiting: list, stopped: Optional[list] = None):
        if not self.aria2:
            return

        queued_items = [item for item in (waiting or []) if item.get("gid")]
        active_items = [item for item in (active or []) if item.get("gid")]
        await self._sync_serial_transfer_gate_impl(active_items, queued_items, stopped)

    def _get_effective_max_concurrent_downloads(self) -> int:
        if self._is_serial_transfer_mode_enabled():
            return self._get_user_max_concurrent_downloads()
        if self._disk_protection_active:
            return max(1, int(self._disk_protection_applied_max_downloads or 1))
        return self._get_user_max_concurrent_downloads()

    async def _apply_aria2_options(self):
        """将本地配置同步到远端 aria2"""
        try:
            cfg = self.config
            aria2_cfg = cfg["aria2"]
            aria2 = self._require_aria2()
            options = {
                "max-concurrent-downloads": str(self._get_effective_max_concurrent_downloads()),
                "split": str(aria2_cfg.get("split", 8)),
                "max-connection-per-server": str(aria2_cfg.get("max_connection_per_server", 8)),
                "min-split-size": f"{int(aria2_cfg.get('min_split_size_mb', 5))}M",
                "max-overall-download-limit": "0",
                "dir": aria2_cfg.get("download_dir", "./downloads"),
            }
            await aria2.change_global_option(options)
        except Exception:
            pass

    async def _sync_disk_space_download_protection(self, active_download_count: int):
        aria2 = self.aria2
        if not aria2:
            return

        if not self._is_disk_protection_enabled():
            configured_max = self._get_user_max_concurrent_downloads()
            should_apply = (
                self._disk_protection_active
                or self._disk_protection_applied_max_downloads != configured_max
            )
            if should_apply:
                try:
                    await aria2.change_global_option({
                        "max-concurrent-downloads": str(configured_max),
                    })
                except Exception as e:
                    logger.warning(f"同步串行模式 aria2 并发配置失败: {e}")
                    return

            self._disk_protection_active = False
            self._disk_protection_applied_max_downloads = configured_max
            self._disk_protection_info = {
                "active": False,
                "message": "",
                "threshold_bytes": self._get_disk_protection_threshold_bytes(),
                "resume_threshold_bytes": self._get_disk_protection_resume_bytes(),
                "configured_max_concurrent": configured_max,
                "applied_max_concurrent": configured_max,
            }
            return

        disk_info = self._disk_usage_info or {}
        if "free" not in disk_info:
            return

        free_bytes = max(0, int(disk_info.get("free") or 0))
        threshold_bytes = self._get_disk_protection_threshold_bytes()
        resume_threshold_bytes = self._get_disk_protection_resume_bytes()
        configured_max = self._get_user_max_concurrent_downloads()
        active_download_count = max(0, int(active_download_count or 0))
        was_active = self._disk_protection_active
        should_protect = free_bytes < (resume_threshold_bytes if was_active else threshold_bytes)
        target_max = configured_max if not should_protect else max(1, active_download_count)
        should_apply = (
            target_max != self._disk_protection_applied_max_downloads
            or should_protect != was_active
        )

        if should_apply:
            try:
                await aria2.change_global_option({
                    "max-concurrent-downloads": str(target_max),
                })
            except Exception as e:
                logger.warning(f"同步磁盘保护状态到 aria2 失败: {e}")
                return

        self._disk_protection_active = should_protect
        self._disk_protection_applied_max_downloads = target_max
        self._disk_protection_info = {
            "active": should_protect,
            "message": "磁盘不足，已自动保护" if should_protect else "",
            "free_bytes": free_bytes,
            "threshold_bytes": threshold_bytes,
            "resume_threshold_bytes": resume_threshold_bytes,
            "configured_max_concurrent": configured_max,
            "applied_max_concurrent": target_max,
        }

        if should_protect and (not was_active or should_apply):
            logger.warning(
                f"磁盘剩余空间不足，已自动保护 aria2 新下载: free={free_bytes}, "
                f"threshold={threshold_bytes}, max_concurrent={target_max}/{configured_max}"
            )
        elif was_active and not should_protect:
            logger.info(
                f"磁盘空间已恢复，已解除 aria2 自动保护: free={free_bytes}, "
                f"restore_max_concurrent={configured_max}"
            )



    async def start(self):
        """启动任务管理器"""
        if self._running and self._monitor_task and not self._monitor_task.done():
            logger.info("任务管理器已在运行，跳过重复启动")
            return
        await db.init_db()
        self._init_clients()
        # 同步配置到 aria2
        await self._apply_aria2_options()
        # 加载已有任务的 GID 到缓存
        all_tasks = await db.get_all_tasks()
        for t in all_tasks:
            if t.get("aria2_gid"):
                self._known_gids.add(t["aria2_gid"])

        if self._is_serial_transfer_mode_enabled():
            await self._normalize_serial_pending_aria2_tasks()

        # 恢复僵死的 uploading 任务（应用重启后 uploading 状态不会自动恢复）
        for t in all_tasks:
            task_id = t["task_id"]
            self._sync_upload_checkpoint_cache(t)
            self._upload_session_state[task_id] = self._normalize_upload_session_state(t.get("upload_session_state"))

            if t["status"] == "uploading":
                local_path = self._get_upload_path(t.get("local_path", ""))
                if local_path and os.path.exists(local_path):
                    token = uuid.uuid4().hex
                    acquired = await db.try_acquire_upload_session(
                        task_id,
                        ("idle", "scheduled", "running"),
                        token,
                        self._session_owner,
                        next_state="scheduled",
                    )
                    if acquired:
                        logger.info(f"恢复僵死的上传任务: {task_id} ({t.get('filename', '?')})")
                        self._upload_session_state[task_id] = "scheduled"
                        upload_t = asyncio.create_task(self._retry_upload(task_id, token))
                        self._upload_tasks[task_id] = upload_t
                else:
                    logger.warning(f"僵死上传任务 {task_id} 本地文件不存在，标记失败")
                    await db.update_task(task_id, status="failed",
                                         error="上传中断且本地文件不存在")

        self._running = True
        # 预热 psutil.cpu_percent()，首次调用返回 0.0，需要先调一次建立基准
        psutil.cpu_percent(interval=None)
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("任务管理器已启动")

    async def stop(self):
        """停止任务管理器"""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
                
        # 取消所有正在进行的异步上传协程
        if self._upload_tasks:
            logger.info(f"正在取消 {len(self._upload_tasks)} 个后台上传任务...")
            for t_id, task in self._upload_tasks.items():
                if not task.done():
                    task.cancel()
            
            # 等待它们安全退出，避免控制台卡死
            await asyncio.gather(*self._upload_tasks.values(), return_exceptions=True)
            self._upload_tasks.clear()

        # 关闭 aria2 HTTP 会话
        aria2 = self.aria2
        if aria2:
            await aria2.close()
        logger.info("任务管理器已停止")



    def register_ws(self, ws):
        """注册 WebSocket 客户端"""
        self._ws_clients.add(ws)

    def unregister_ws(self, ws):
        """注销 WebSocket 客户端"""
        self._ws_clients.discard(ws)

    async def broadcast(self, message: dict):
        """向所有 WebSocket 客户端广播消息"""
        dead = set()
        for ws in self._ws_clients:
            try:
                await asyncio.wait_for(ws.send_json(message), timeout=3)
            except Exception:
                dead.add(ws)
        self._ws_clients -= dead

    def _set_runtime_task_fields(self, task_id: str, **kwargs):

        task_id = str(task_id or "")
        if not task_id:
            return
        state = dict(self._runtime_task_state.get(task_id) or {})
        for key, value in kwargs.items():
            if value is None:
                state.pop(key, None)
            else:
                state[key] = value
        if state:
            self._runtime_task_state[task_id] = state
        else:
            self._runtime_task_state.pop(task_id, None)

    def _clear_runtime_task_fields(self, task_id: str, *keys: str):
        task_id = str(task_id or "")
        if not task_id:
            return
        if not keys:
            self._runtime_task_state.pop(task_id, None)
            return
        state = self._runtime_task_state.get(task_id)
        if not state:
            return
        for key in keys:
            state.pop(key, None)
        if state:
            self._runtime_task_state[task_id] = state
        else:
            self._runtime_task_state.pop(task_id, None)

    def _merge_runtime_task_fields(self, task: Optional[dict]) -> Optional[dict]:
        if not task:
            return None
        merged = dict(task)
        merged.pop("aria2_options_json", None)
        task_id = str(merged.get("task_id") or "")
        confirmed_chunks = self._coerce_upload_checkpoint_value(merged.get("upload_confirmed_chunks"))
        confirmed_total = max(0, int(merged.get("upload_confirmed_total") or 0))
        if confirmed_chunks > 0 or confirmed_total > 0:
            self._upload_confirmed_checkpoints[task_id] = max(
                self._coerce_upload_checkpoint_value(self._upload_confirmed_checkpoints.get(task_id)),
                confirmed_chunks,
            )
            merged["upload_chunk_done"] = confirmed_chunks
            merged["upload_chunk_total"] = confirmed_total
        runtime_fields = self._runtime_task_state.get(str(merged.get("task_id") or ""))
        if runtime_fields:
            merged.update(runtime_fields)

        if str(merged.get("status") or "") == "completed":
            total = max(
                confirmed_total,
                int(self._coerce_upload_checkpoint_value(merged.get("upload_chunk_total"))),
                int(self._coerce_upload_checkpoint_value(merged.get("upload_chunk_done"))),
            )
            if total > 0:
                merged["upload_chunk_done"] = float(total)
                merged["upload_chunk_total"] = total
        elif str(merged.get("status") or "") == "cancelled":
            for key in ("upload_note", "upload_note_level", "upload_chunk_done", "upload_chunk_total"):
                merged.pop(key, None)

        return merged


    def _count_file_chunks(self, file_size: int) -> int:
        if file_size <= 0:
            return 0
        chunk_size = max(1, int(getattr(self.teldrive, "chunk_size", 0) or 1))
        return int((file_size + chunk_size - 1) // chunk_size)

    def _count_path_chunks(self, local_path: str) -> int:
        if not local_path or not os.path.exists(local_path):
            return 0
        if os.path.isfile(local_path):
            return self._count_file_chunks(os.path.getsize(local_path))

        total_chunks = 0
        for root, _dirs, filenames in os.walk(local_path):
            for fname in filenames:
                full_path = os.path.join(root, fname)
                try:
                    total_chunks += self._count_file_chunks(os.path.getsize(full_path))
                except OSError:
                    continue
        return total_chunks

    def track_upload_progress(self, task_id: str, uploaded_bytes: int):

        self._task_uploaded_bytes[task_id] = max(0, int(uploaded_bytes or 0))

    def clear_upload_progress(self, task_id: str):
        self._task_uploaded_bytes.pop(task_id, None)

    @staticmethod
    def _coerce_upload_checkpoint_value(value) -> float:
        try:
            return max(0.0, float(value or 0))
        except Exception:
            return 0.0

    def _get_upload_progress_checkpoint(self, task: Optional[dict]) -> float:
        task = self._merge_runtime_task_fields(task)
        if not task:
            return 0.0
        chunk_done = self._coerce_upload_checkpoint_value(task.get("upload_chunk_done"))
        if chunk_done > 0:
            return chunk_done
        return self._coerce_upload_checkpoint_value(task.get("upload_progress"))

    def _record_upload_progress_checkpoint(self, task_id: str, task: Optional[dict] = None) -> float:
        checkpoint = self._get_upload_progress_checkpoint(task)
        if checkpoint > 0:
            previous = self._coerce_upload_checkpoint_value(
                self._upload_confirmed_checkpoints.get(task_id)
            )
            self._upload_confirmed_checkpoints[task_id] = max(previous, checkpoint)
        return self._coerce_upload_checkpoint_value(
            self._upload_confirmed_checkpoints.get(task_id)
        )

    def _reset_upload_retry_state(self, task_id: str):
        self._upload_retry_counts.pop(task_id, None)
        self._upload_retry_checkpoints.pop(task_id, None)
        self._upload_confirmed_checkpoints.pop(task_id, None)

    @staticmethod
    def _normalize_upload_session_state(value) -> str:
        state = str(value or "idle").strip().lower()
        if state in {"scheduled", "running", "completed", "failed_cleanup_required"}:
            return state
        return "idle"

    def _sync_upload_checkpoint_cache(self, task: Optional[dict]) -> float:
        if not task:
            return 0.0
        task_id = str(task.get("task_id") or "")
        confirmed = self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks"))
        if task_id and confirmed > 0:
            previous = self._coerce_upload_checkpoint_value(self._upload_confirmed_checkpoints.get(task_id))
            self._upload_confirmed_checkpoints[task_id] = max(previous, confirmed)
        return confirmed

    async def _refresh_upload_session_from_db(self, task_id: str, task: Optional[dict] = None) -> dict:
        current = dict(task or await db.get_task(task_id) or {})
        if not current:
            return {}
        confirmed = self._sync_upload_checkpoint_cache(current)
        self._set_runtime_task_fields(
            task_id,
            upload_chunk_done=confirmed,
            upload_chunk_total=max(0, int(current.get("upload_confirmed_total") or 0)),
        )
        self._upload_session_state[task_id] = self._normalize_upload_session_state(current.get("upload_session_state"))
        return current

    @staticmethod
    def _format_structured_upload_error(error_code: str, message: str, details: Optional[dict] = None) -> str:
        payload = {"code": error_code, "message": message, "details": details or {}}
        return f"structured_upload_error::{json.dumps(payload, ensure_ascii=False)}"

    @staticmethod
    def _parse_structured_upload_error(error) -> Optional[dict]:
        text = str(error or "")
        prefix = "structured_upload_error::"
        if not text.startswith(prefix):
            return None
        try:
            payload = json.loads(text[len(prefix):])
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def _is_polluted_upload_error(self, error) -> bool:
        payload = self._parse_structured_upload_error(error)
        if not payload:
            return False
        code = str(payload.get("code") or "")
        return code.startswith("remote_parts_") or code == "upload_session_conflict"

    def _set_upload_session_state(self, task_id: str, state: Optional[str], meta: Optional[dict] = None):
        if state:
            self._upload_session_state[task_id] = state
        else:
            self._upload_session_state.pop(task_id, None)
        if meta is not None:
            self._upload_session_meta[task_id] = dict(meta)

    def _clear_upload_session_state(self, task_id: str, *, keep_meta: bool = False):
        self._upload_session_state.pop(task_id, None)
        if not keep_meta:
            self._upload_session_meta.pop(task_id, None)

    def _mark_upload_session_scheduled(self, task_id: str) -> bool:
        if self._upload_session_state.get(task_id) in {"scheduled", "running", "finalized"}:
            return False
        self._upload_session_state[task_id] = "scheduled"
        return True

    def _mark_upload_session_running(self, task_id: str) -> bool:
        current = self._upload_session_state.get(task_id)
        if current == "finalized":
            return False
        if current not in {None, "scheduled", "running"}:
            return False
        self._upload_session_state[task_id] = "running"
        return True

    def _mark_upload_session_finalized(self, task_id: str):
        self._upload_session_state[task_id] = "finalized"

    def _store_upload_session_meta(self, task_id: str, meta: Optional[dict]):
        if meta is not None:
            self._upload_session_meta[task_id] = dict(meta)

    def _get_upload_session_meta(self, task_id: str) -> dict:
        return dict(self._upload_session_meta.get(task_id) or {})

    def _clear_upload_retry_budget(self, task_id: str):
        self._upload_retry_counts.pop(task_id, None)
        self._upload_retry_checkpoints.pop(task_id, None)

    def _get_upload_display_baseline(self, task_id: str, total_chunks: int = 0) -> float:
        baseline = self._coerce_upload_checkpoint_value(self._upload_confirmed_checkpoints.get(task_id))
        if total_chunks > 0:
            return min(baseline, float(total_chunks))
        return baseline

    @staticmethod
    def _is_upload_stage_task(task: dict) -> bool:
        return (
            float(task.get("download_progress") or 0) >= 100
            or float(task.get("upload_progress") or 0) > 0
        )

    @staticmethod
    def _should_skip_auto_retry(task: dict) -> bool:
        error = str(task.get("error") or "")
        blocked_markers = (
            "用户手动暂停上传",
            "本地文件不存在",
        )

        return error.startswith("structured_upload_error::") or any(marker in error for marker in blocked_markers)

    @staticmethod
    def _normalize_teldrive_path(path: str) -> str:
        path = str(path or "/").strip().replace("\\", "/")
        if not path:
            return "/"
        if not path.startswith("/"):
            path = "/" + path
        while "//" in path:
            path = path.replace("//", "/")
        return path.rstrip("/") or "/"

    def _get_task_teldrive_path(self, task: dict, local_path: str) -> str:
        configured_target = self._normalize_teldrive_path(
            self.config["teldrive"].get("target_path", "/")
        )
        task_target = self._normalize_teldrive_path(
            task.get("teldrive_path") or configured_target
        )
        if task_target != configured_target:
            return task_target
        return self._calc_teldrive_path(local_path)

    def _get_configured_connection_limit(self) -> int:
        aria2_cfg = self.config.get("aria2", {})
        split = max(0, int(aria2_cfg.get("split") or 0))
        per_server = max(0, int(aria2_cfg.get("max_connection_per_server") or 0))
        return max(split, per_server)

    def _build_download_runtime_fields(self, parsed: dict, task_status: str) -> dict:
        total_bytes = max(0, int(parsed.get("total_length") or 0))
        downloaded_bytes = max(0, int(parsed.get("completed_length") or 0))
        if total_bytes > 0:
            downloaded_bytes = min(downloaded_bytes, total_bytes)
        current_connections = max(0, int(parsed.get("connections") or 0))
        max_connections = max(current_connections, self._get_configured_connection_limit())
        return {
            "downloaded_text": parsed.get("downloaded_text") or "0 B",
            "downloaded_bytes": downloaded_bytes,
            "total_text": parsed.get("total_text") or parsed.get("file_size") or "0 B",
            "total_bytes": total_bytes,
            "eta_text": parsed.get("eta_text") or "" if task_status == "downloading" else "",
            "connections": current_connections if task_status in ("downloading", "paused") else 0,
            "max_connections": max_connections if task_status in ("downloading", "paused") else 0,
        }

    def get_global_stat(self) -> dict:


        """获取当前缓存的全局统计数据（供 WS init 立即推送）"""
        data = {
            "download_speed": int(self._last_download_speed),
            "download_speed_detail": {
                "aria2": int(self._last_download_speed),
            },
            "upload_speed": int(self._upload_speed),
        }

        if self._disk_usage_info:
            data["disk"] = self._disk_usage_info
        if self._cpu_info:
            data["cpu"] = self._cpu_info
        if self._disk_protection_info:
            data["download_protection"] = self._disk_protection_info
        return data




    # ===========================================
    # 核心：监控循环 — 主动轮询 aria2 全部任务
    # ===========================================

    async def _monitor_loop(self):
        """定期轮询 aria2，同步所有下载任务到数据库和前端"""
        import time
        self._upload_time_snapshot = time.monotonic()
        self._upload_total_snapshot = 0
        self._last_cleanup_time = 0.0
        while self._running:
            try:
                # 计算总上传速度
                now = time.monotonic()
                elapsed = now - self._upload_time_snapshot
                if elapsed >= 2.0:
                    current_total = sum(self._task_uploaded_bytes.values())
                    self._upload_speed = (current_total - self._upload_total_snapshot) / elapsed
                    if self._upload_speed < 0:
                        self._upload_speed = 0.0
                    self._upload_total_snapshot = current_total
                    self._upload_time_snapshot = now

                # 每个步骤独立保护，单步失败不影响其他
                # 先采集 CPU / 磁盘状态，再同步任务和广播

                try:
                    await self._check_cpu_usage()
                except Exception as e:
                    logger.debug(f"CPU 检测异常: {e}")

                try:
                    await self._check_disk_usage()
                except Exception as e:
                    logger.debug(f"磁盘检测异常: {e}")

                try:
                    await self._sync_aria2_tasks()
                except Exception as e:
                    logger.warning(f"任务同步异常: {e}")
                    # DB 连接可能异常，尝试重建
                    try:
                        await db.reconnect_db()
                    except Exception:
                        pass

                # 独立广播 global_stat（不依赖 aria2 是否连接成功）
                try:
                    await self.broadcast({
                        "type": "global_stat",
                        "data": self.get_global_stat()
                    })
                except Exception:
                    pass


                # 定期兜底清理已完成任务的残留本地文件（每 60 秒）
                if now - self._last_cleanup_time >= 60:
                    self._last_cleanup_time = now
                    try:
                        await self._cleanup_completed_files()
                    except Exception as e:
                        logger.debug(f"清理异常: {e}")

                # 定期自动重试失败的上传任务（每 30 秒）
                if now - self._last_retry_time >= 30:
                    self._last_retry_time = now
                    try:
                        await self._auto_retry_failed_uploads()
                    except Exception as e:
                        logger.debug(f"自动重试异常: {e}")

                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(5)

    async def _check_disk_usage(self):
        """采集磁盘使用信息，供前端仪表盘显示"""
        try:
            download_dir = get_download_dir(self.config)
            usage = shutil.disk_usage(download_dir)
        except Exception as e:
            logger.debug(f"检测磁盘使用失败: {e}")
            return

        total_bytes = int(usage.total or 0)
        used_bytes = int(usage.used or 0)
        free_bytes = int(usage.free or 0)
        total_gb = round(total_bytes / (1024 ** 3), 2)
        used_gb = round(used_bytes / (1024 ** 3), 2)
        free_gb = round(free_bytes / (1024 ** 3), 2)
        percent = round(used_bytes / total_bytes * 100, 1) if total_bytes > 0 else 0

        self._disk_usage_info = {
            "total": total_bytes,
            "used": used_bytes,
            "free": free_bytes,
            "total_gb": total_gb,
            "used_gb": used_gb,
            "free_gb": free_gb,
            "percent": percent,
        }

    async def _check_cpu_usage(self):
        """采集 CPU 使用率，供前端仪表盘显示"""
        try:
            cpu_pct = psutil.cpu_percent(interval=None)
        except Exception as e:
            logger.debug(f"检测 CPU 使用失败: {e}")
            return

        self._cpu_info = {
            "percent": round(cpu_pct, 1),
        }


    async def _sync_aria2_tasks(self):
        """从 aria2 获取所有任务，同步到本地数据库"""
        try:
            # 获取 aria2 全部任务
            aria2 = self._require_aria2()
            active = await aria2.tell_active() or []
            waiting = await aria2.tell_waiting(0, 1000) or []
            # 分页拉取所有 stopped 任务，避免超过 100 条后遗漏
            stopped = await aria2.tell_stopped_all() or []
        except Exception as e:
            # aria2 连接失败时静默跳过（仅每 30 秒打一次日志）
            self._last_download_speed = 0
            logger.debug(f"aria2 轮询失败: {e}")
            return

        try:
            await self._sync_disk_space_download_protection(len(active))
        except Exception as e:
            logger.debug(f"同步磁盘保护状态失败: {e}")

        removed_serial_gids: set[str] = set()
        try:
            removed_serial_gids = await self._normalize_serial_pending_aria2_tasks(active, waiting, stopped)
        except Exception as e:
            logger.debug(f"normalize serial pending aria2 tasks failed: {e}")

        if removed_serial_gids:
            active = [item for item in active if item.get("gid") not in removed_serial_gids]
            waiting = [item for item in waiting if item.get("gid") not in removed_serial_gids]
            stopped = [item for item in stopped if item.get("gid") not in removed_serial_gids]

        try:
            await self._sync_serial_transfer_gate(active, waiting, stopped)
        except Exception as e:
            logger.debug(f"同步串行下载上传状态失败: {e}")

        all_aria2_tasks = active + waiting + stopped
        tracked_download_speed = 0


        for item in all_aria2_tasks:

            gid = item.get("gid", "")
            if not gid:
                continue

            # 终态任务不再处理，直接跳过
            if gid in self._terminal_gids:
                continue

            parsed = self._parse_aria2_item(item)
            aria2_status = parsed["status"]
            task_dir = item.get("dir", "")

            # 用 aria2 任务级的 dir + 文件名重新构造本地路径
            # 不信任 files[0].path 中的目录部分（aria2 可能返回错误的目录）
            bt_name = item.get("bittorrent", {}).get("info", {}).get("name", "")
            if bt_name and task_dir:
                # BT 下载：dir + bt_name
                parsed["file_path"] = os.path.join(task_dir, bt_name)
                parsed["filename"] = bt_name
            elif task_dir and parsed["filename"]:
                # 非 BT 下载：dir + filename
                parsed["file_path"] = os.path.join(task_dir, parsed["filename"])

            # 判断是否已入库
            if gid not in self._known_gids:
                # 新发现的 aria2 任务，检查是否已在数据库
                existing = await db.get_task_by_gid(gid)
                if not existing:
                    # GID 可能直接作为 task_id 存在（如重启后）
                    existing = await db.get_task(gid)
                if existing:
                    self._known_gids.add(gid)
                else:
                    if aria2_status in ("error", "removed"):
                        self._terminal_gids.add(gid)
                        logger.info(f"跳过历史 aria2 终态任务: {gid} ({parsed['filename']}) 状态={aria2_status}")
                        continue


                    task_id = gid  # 直接用 GID 作为 task_id
                    url = ""
                    files = item.get("files", [])
                    if files:
                        uris = files[0].get("uris", [])
                        if uris:
                            url = uris[0].get("uri", "")

                    initial_status = self._visible_aria2_status(aria2_status, gid)

                    await db.add_task(
                        task_id=task_id,
                        url=url,
                        filename=parsed["filename"],
                        teldrive_path=self.config["teldrive"].get("target_path", "/")
                    )
                    await db.update_task(
                        task_id,
                        status=initial_status,
                        aria2_gid=gid,
                        download_progress=100.0 if aria2_status == "complete" else parsed["progress"],
                        download_speed=parsed["speed_str"] if initial_status == "downloading" else "",
                        file_size=parsed["file_size"],
                        local_path=parsed["file_path"]
                    )
                    self._set_runtime_task_fields(
                        task_id,
                        **self._build_download_runtime_fields(parsed, initial_status)
                    )
                    self._known_gids.add(gid)

                    if initial_status == "downloading":
                        tracked_download_speed += int(parsed["download_speed"] or 0)
                    logger.info(f"发现 aria2 任务: {gid} ({parsed['filename']}) 状态={initial_status}")
                    await self._broadcast_task_update(task_id)

                    if initial_status in ("failed", "cancelled"):
                        self._terminal_gids.add(gid)

                    if aria2_status == "complete":
                        if parsed["file_path"]:
                            await self._schedule_upload_from_complete(task_id, gid)
                        else:
                            await db.update_task(task_id, status="completed")
                            self._terminal_gids.add(gid)
                            await self._broadcast_task_update(task_id)
                    continue




            # 已入库的任务，更新状态
            task = await db.get_task_by_gid(gid)
            if not task:
                continue

            task_id = task["task_id"]
            current_status = task["status"]
            if (
                self._is_serial_transfer_mode_enabled()
                and aria2_status == "paused"
                and current_status == "pending"
            ):
                self._serial_gate_paused_gids.add(gid)

            # 已完成上传、已取消、已失败的任务不再更新
            if current_status in ("completed", "cancelled", "failed"):
                self._terminal_gids.add(gid)
                continue

            # 正在上传中的任务不更新下载状态
            if current_status == "uploading":
                continue

            update_data = {
                "download_progress": parsed["progress"],
                "download_speed": parsed["speed_str"],
                "file_size": parsed["file_size"],
            }
            if parsed["filename"]:
                update_data["filename"] = parsed["filename"]
            if parsed["file_path"]:
                update_data["local_path"] = parsed["file_path"]

            visible_status = self._visible_aria2_status(aria2_status, gid)
            if visible_status == "downloading":
                update_data["status"] = "downloading"
                tracked_download_speed += int(parsed["download_speed"] or 0)
            elif visible_status == "pending":

                update_data["status"] = "pending"
                update_data["download_speed"] = ""
            elif visible_status == "paused":
                update_data["status"] = "paused"
                update_data["download_speed"] = ""
            elif visible_status == "uploading":
                update_data["status"] = "uploading"
                update_data["download_progress"] = 100.0
                update_data["download_speed"] = ""
            elif visible_status == "failed":
                error_code = item.get("errorCode", "")
                error_msg = item.get("errorMessage", "下载失败")
                update_data["status"] = "failed"
                update_data["error"] = f"aria2 错误 [{error_code}]: {error_msg}"
            elif visible_status == "cancelled":
                update_data["status"] = "cancelled"

            await db.update_task(task_id, **update_data)
            self._set_runtime_task_fields(
                task_id,
                **self._build_download_runtime_fields(parsed, update_data.get("status") or current_status)
            )
            # 合并更新数据到已有 task 记录用于广播，避免再次查库
            task.update(update_data)
            await self._broadcast_task_update(task_id, task)


            # 下载完成 → 触发上传
            if aria2_status == "complete" and current_status != "uploading":
                local_path = parsed["file_path"]
                if local_path:
                    await self._schedule_upload_from_complete(task_id, gid)
                else:
                    await db.update_task(task_id, status="completed")
                    self._terminal_gids.add(gid)
                    await self._broadcast_task_update(task_id)

        try:
            await self._dispatch_next_serial_download(active, waiting, stopped)
        except Exception as e:
            logger.debug(f"serial dispatcher failed: {e}")

        self._last_download_speed = tracked_download_speed

    def _calc_teldrive_path(self, local_path: str) -> str:

        """计算文件在 TelDrive 上的目标目录，保留下载目录中的子目录结构。"""
        target_path = self.config["teldrive"].get("target_path", "/")

        # 确定基础目录（upload_dir 或 download_dir）
        upload_dir = self.config["teldrive"].get("upload_dir", "").strip()
        if upload_dir:
            base_dir = os.path.normpath(upload_dir)
        else:
            base_dir = os.path.normpath(
                self.config["aria2"].get("download_dir", "./downloads"))

        norm_path = os.path.normpath(local_path)

        # 文件 → 取父目录；目录（BT文件夹）→ 取自身
        if os.path.isfile(norm_path):
            parent = os.path.dirname(norm_path)
        else:
            parent = norm_path

        norm_parent = os.path.normpath(parent)

        if norm_parent == base_dir:
            # 文件直接在下载目录下，无子目录
            result = target_path
        elif norm_parent.startswith(base_dir + os.sep):
            rel = os.path.relpath(norm_parent, base_dir).replace("\\", "/")
            result = target_path.rstrip("/") + "/" + rel
        else:
            result = target_path

        logger.info(f"[路径] {local_path} -> teldrive={result}")
        return result

    async def _wait_upload_slot(self):
        """等待可用的上传槽位（动态读取配置的并发数）"""
        while True:
            max_uploads = self._get_effective_upload_concurrency()
            if self._active_uploads < max_uploads:
                self._active_uploads += 1
                return
            self._upload_slot_event.clear()
            await self._upload_slot_event.wait()

    def _release_upload_slot(self):
        """释放一个上传槽位"""
        self._active_uploads = max(0, self._active_uploads - 1)
        self._upload_slot_event.set()

    @staticmethod
    def _calc_upload_timeout(file_size: int) -> int:

        """根据文件大小动态计算上传超时（秒）。

        保底 600s (10分钟) + 每 GB 额外 600s (10分钟)。
        例: 8GB → 600 + 8*600 = 5400s ≈ 90 分钟
        """
        base = 600
        extra = int(file_size / (1024 ** 3)) * 600
        return base + extra

    async def _schedule_upload_from_complete(self, task_id: str, gid: str) -> bool:
        task = await db.get_task(task_id)
        if not task:
            return False
        await self._refresh_upload_session_from_db(task_id, task)
        if (
            task.get("status") == "completed"
            or task.get("upload_finished_at")
            or self._normalize_upload_session_state(task.get("upload_session_state")) in {"scheduled", "running", "completed"}
        ):
            self._terminal_gids.add(gid)
            return False
        token = uuid.uuid4().hex
        acquired = await db.try_acquire_upload_session(
            task_id,
            ("idle",),
            token,
            self._session_owner,
            next_state="scheduled",
        )
        if not acquired:
            return False
        self._upload_session_state[task_id] = "scheduled"
        self._upload_tasks[task_id] = asyncio.create_task(self._handle_download_complete(task_id, gid, token))
        return True

    async def _delete_telegram_messages(self, message_ids) -> bool:
        normalized_ids = []
        for msg_id in message_ids or []:
            try:
                normalized_ids.append(int(msg_id))
            except Exception:
                continue
        if not normalized_ids:
            return True
        try:
            from app.modules.tel2teldrive.service import config_store, remember_internal_deleted_message_ids, service as t2td_service
            config = config_store.runtime()
            client = getattr(t2td_service, "client", None)
            if client is None or not client.is_connected():
                return False
            remember_internal_deleted_message_ids(normalized_ids)
            await client.delete_messages(config.telegram_channel_id, normalized_ids)
            return True
        except Exception as e:
            logger.warning(f"failed to delete polluted upload telegram messages: {e}")
            return False

    async def cleanup_polluted_upload(self, task_id: str, retry_after_cleanup: bool = False) -> dict:
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "task not found"}
        if not self._is_polluted_upload_error(task.get("error")):
            return {"success": False, "message": "task has no polluted upload session to clean"}

        meta = self._get_upload_session_meta(task_id)
        upload_id = str(meta.get("upload_id") or "")
        remote_parts = list(meta.get("remote_parts") or [])
        if not upload_id and not remote_parts:
            return {"success": False, "message": "missing polluted upload session metadata"}

        teldrive = self._require_teldrive()
        if upload_id:
            try:
                fetched_parts = await teldrive.get_upload_parts(upload_id)
                if fetched_parts:
                    remote_parts = fetched_parts
            except Exception:
                pass

        message_ids = []
        for part in remote_parts:
            message_id = teldrive._get_part_message_id(part)
            if message_id is not None:
                message_ids.append(message_id)
        if message_ids and not await self._delete_telegram_messages(message_ids):
            return {"success": False, "message": "failed to delete polluted upload chunks; ensure tel2teldrive is connected"}

        if upload_id:
            await teldrive.cleanup_upload_session(upload_id)
        self._clear_upload_retry_budget(task_id)
        self._clear_upload_session_state(task_id)
        self._upload_confirmed_checkpoints[task_id] = 0.0
        self._set_runtime_task_fields(
            task_id,
            upload_chunk_done=0,
            upload_chunk_total=0,
            upload_note="polluted remote chunks cleaned; ready to retry upload",
            upload_note_level="warning",
        )
        await db.update_task(
            task_id,
            status="failed",
            error="polluted remote chunks cleaned; retry upload",
            upload_session_state="idle",
            upload_session_token=None,
            upload_session_owner=None,
            upload_confirmed_chunks=0,
            upload_confirmed_total=0,
            upload_finished_at=None,
        )
        await self._broadcast_task_update(task_id)

        if retry_after_cleanup:
            return await self.retry_task(task_id)
        return {"success": True, "message": f"cleaned {len(message_ids)} polluted remote chunks from this upload session"}

    async def _handle_download_complete(self, task_id: str, gid: str, token: str):
        """下载完成后自动上传到 TelDrive。"""
        if not await db.confirm_upload_session_running(task_id, token):
            logger.info(f"skip duplicate upload runner for task {task_id} gid={gid}, token={token}")
            self._upload_tasks.pop(task_id, None)
            await self._refresh_upload_session_from_db(task_id)
            return
        self._upload_session_state[task_id] = "running"
        if gid in self._uploading_gids:
            await db.release_upload_session(task_id, token, "idle", keep_checkpoint=True)
            return
        self._uploading_gids.add(gid)
        finalized = False
        keep_session_meta = False
        await self._wait_upload_slot()
        try:
            task = await self._refresh_upload_session_from_db(task_id)
            if not task or not task.get("local_path") or task.get("upload_finished_at") or task.get("status") == "completed":
                logger.warning(f"task {task_id} missing local_path, skip upload")
                return
            local_path = self._get_upload_path(task["local_path"])
            teldrive_path = self._get_task_teldrive_path(task, local_path)
            for attempt in range(5):
                if os.path.exists(local_path):
                    break
                await asyncio.sleep(1)
            if not os.path.exists(local_path):
                self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
                await db.update_task(task_id, status="failed", error=f"local file missing: {local_path}")
                await db.release_upload_session(task_id, token, "idle", error=f"local file missing: {local_path}")
                await self._broadcast_task_update(task_id)
                return
            total_chunks = self._count_path_chunks(local_path)
            baseline_chunks = min(
                self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
                float(total_chunks) if total_chunks > 0 else self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
            )
            self._upload_confirmed_checkpoints[task_id] = baseline_chunks
            self._set_runtime_task_fields(task_id, upload_chunk_done=baseline_chunks, upload_chunk_total=total_chunks, upload_note=None, upload_note_level=None)
            upload_progress = round(baseline_chunks / total_chunks * 100, 1) if total_chunks > 0 else 0.0
            await db.update_task(task_id, status="uploading", download_progress=100.0, upload_progress=upload_progress, download_speed="", upload_speed="", error=None)
            await self._broadcast_task_update(task_id)
            if os.path.isdir(local_path):
                await self._upload_directory(task_id, local_path, teldrive_path, token)
            else:
                await self._upload(task_id, local_path, teldrive_path, token)
            await self._auto_delete_local(task_id, local_path)
            await self._check_disk_usage()
            finalized_task = await db.get_task(task_id)
            finalized = bool(finalized_task and finalized_task.get("upload_finished_at"))
        except asyncio.CancelledError:
            logger.info(f"task {task_id} upload cancelled")
        except Exception as e:
            logger.error(f"task {task_id} upload failed: {e}")
            task_after_error = await db.get_task(task_id)
            if task_after_error and task_after_error.get("status") != "completed":
                keep_session_meta = self._is_polluted_upload_error(str(e))
                self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
                next_state = "failed_cleanup_required" if keep_session_meta else "idle"
                await db.update_task(task_id, status="failed", error=str(e), upload_session_state=next_state)
                await db.release_upload_session(
                    task_id,
                    token,
                    next_state,
                    error=str(e),
                    keep_checkpoint=True,
                )
                await self._broadcast_task_update(task_id)
        finally:
            self._release_upload_slot()
            self._uploading_gids.discard(gid)
            self._upload_tasks.pop(task_id, None)
            if finalized:
                self._mark_upload_session_finalized(task_id)
            else:
                self._clear_upload_session_state(task_id, keep_meta=keep_session_meta)
            try:
                await self._dispatch_next_serial_download()
            except Exception as e:
                logger.debug(f"serial dispatcher after upload failed: {e}")

    async def _auto_delete_local(self, task_id: str, local_path: str) -> bool:
        """上传成功后自动删除本地文件（如果配置了 auto_delete）"""
        try:
            task = await db.get_task(task_id)
            if not task or task["status"] != "completed":
                return False
            if not self.config.get("upload", {}).get("auto_delete", True):
                return True
            if not local_path or not os.path.exists(local_path):
                await self._check_disk_usage()
                return True
            # 带重试的删除（Windows 可能因句柄延迟释放而失败）
            for attempt in range(3):
                try:
                    if os.path.isdir(local_path):
                        shutil.rmtree(local_path)
                        logger.info(f"已删除本地文件夹: {local_path}")
                    else:
                        os.remove(local_path)
                        logger.info(f"已删除本地文件: {local_path}")
                    await self._check_disk_usage()
                    return True
                except PermissionError:
                    if attempt < 2:
                        logger.warning(f"删除文件被拒绝(句柄占用)，{attempt+1}/3 次重试: {local_path}")
                        await asyncio.sleep(2)
                    else:
                        raise
        except Exception as e:
            logger.warning(f"删除本地文件失败: {local_path}, {e}")

        return False

    async def _cleanup_completed_files(self):
        """定期清理已完成任务的本地残留文件（兜底机制）"""
        if not self.config.get("upload", {}).get("auto_delete", True):
            return
        try:
            all_tasks = await db.get_all_tasks()
            max_retries = self.config.get("upload", {}).get("max_retries", 3)
            cleaned_any = False
            for task in all_tasks:
                status = task["status"]
                task_id = task["task_id"]

                # 已完成：清理残留文件
                should_clean = (status == "completed")

                # 失败且重试次数耗尽：清理文件释放磁盘
                if status == "failed":
                    retries = self._upload_retry_counts.get(task_id, 0)
                    if retries >= max_retries:
                        should_clean = True

                if not should_clean:
                    continue

                local_path = task.get("local_path", "")
                if not local_path:
                    continue
                local_path = self._get_upload_path(local_path)
                if not local_path or not os.path.exists(local_path):
                    continue
                label = "失败(重试耗尽)" if status == "failed" else "已完成"
                logger.info(f"兜底清理：删除{label}任务的残留文件: {local_path}")
                try:
                    if os.path.isdir(local_path):
                        shutil.rmtree(local_path)
                    else:
                        os.remove(local_path)
                    cleaned_any = True
                    logger.info(f"兜底清理成功: {local_path}")
                except Exception as e:
                    logger.warning(f"兜底清理失败: {local_path}, {e}")
            if cleaned_any:
                await self._check_disk_usage()
                await self._dispatch_next_serial_download()
        except Exception as e:
            logger.debug(f"清理文件异常: {e}")

    async def _auto_retry_failed_uploads(self):
        """自动重试失败的上传任务，超过 max_retries 次后放弃并清理本地文件"""
        max_retries = self.config.get("upload", {}).get("max_retries", 3)
        try:
            all_tasks = await db.get_all_tasks()
            for raw_task in all_tasks:
                task = self._merge_runtime_task_fields(raw_task)
                if not task:
                    continue
                if task["status"] != "failed":
                    continue
                if not self._is_upload_stage_task(task):
                    continue
                if self._should_skip_auto_retry(task):
                    continue

                task_id = task["task_id"]

                # 已经在重试中的跳过
                if task_id in self._upload_tasks:
                    continue

                # 没有本地文件的跳过（不是上传失败）
                local_path = task.get("local_path", "")
                if not local_path:
                    continue
                local_path = self._get_upload_path(local_path)
                if not local_path or not os.path.exists(local_path):
                    continue

                current_checkpoint = max(
                    self._record_upload_progress_checkpoint(task_id, task),
                    self._get_upload_progress_checkpoint(task),
                )
                last_retry_checkpoint = self._coerce_upload_checkpoint_value(
                    self._upload_retry_checkpoints.get(task_id)
                )
                if current_checkpoint > last_retry_checkpoint:
                    self._upload_retry_counts[task_id] = 0
                    self._upload_retry_checkpoints[task_id] = current_checkpoint

                retries = self._upload_retry_counts.get(task_id, 0)
                if retries >= max_retries:
                    # 重试耗尽，跳过（文件清理由 _cleanup_completed_files 处理）
                    continue

                # 发起重试
                next_retry = retries + 1
                self._upload_retry_counts[task_id] = next_retry
                self._upload_retry_checkpoints[task_id] = current_checkpoint
                total_chunks = self._count_path_chunks(local_path)
                baseline_chunks = min(
                    self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
                    float(total_chunks) if total_chunks > 0 else self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
                )
                self._upload_confirmed_checkpoints[task_id] = baseline_chunks
                baseline_progress = round(baseline_chunks / total_chunks * 100, 1) if total_chunks > 0 else 0.0
                retry_message = f"上传失败，正在自动重试（{next_retry}/{max_retries}），等待上传槽位..."
                self._set_runtime_task_fields(
                    task_id,
                    upload_chunk_done=baseline_chunks,
                    upload_chunk_total=total_chunks,
                    upload_note=retry_message,
                    upload_note_level="warning",
                )
                await db.update_task(task_id, status="uploading", upload_progress=baseline_progress, upload_speed="", error=None)
                await self._broadcast_task_update(task_id)
                logger.info(f"自动重试上传任务 {task_id} ({next_retry}/{max_retries})")
                self._clear_upload_session_state(task_id, keep_meta=True)
                token = uuid.uuid4().hex
                acquired = await db.try_acquire_upload_session(
                    task_id,
                    ("idle",),
                    token,
                    self._session_owner,
                    next_state="scheduled",
                )
                if acquired and self._mark_upload_session_scheduled(task_id):
                    t = asyncio.create_task(self._retry_upload(task_id, token))
                    self._upload_tasks[task_id] = t



        except Exception as e:
            logger.debug(f"自动重试扫描异常: {e}")

    # ===========================================
    # 上传
    # ===========================================

    async def _upload_directory(self, task_id: str, dir_path: str, teldrive_path: str = "/", token: Optional[str] = None):
        import time
        teldrive = self._require_teldrive()
        base_teldrive_path = teldrive_path.rstrip("/") if teldrive_path != "/" else "/"
        all_files = []
        for root, _dirs, filenames in os.walk(dir_path):
            for fname in filenames:
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, dir_path)
                file_size = os.path.getsize(full_path)
                all_files.append((full_path, rel_path, file_size))
        if not all_files:
            self._mark_upload_session_finalized(task_id)
            await db.update_task(task_id, status="completed", upload_progress=100.0)
            await self._broadcast_task_update(task_id)
            return
        total_size = sum(s for _, _, s in all_files)
        total_chunks = sum(self._count_file_chunks(s) for _, _, s in all_files)
        uploaded_total = [0]
        baseline_chunks = self._get_upload_display_baseline(task_id, total_chunks)
        confirmed_chunks_total = [baseline_chunks]
        _last_broadcast = [0.0]
        _last_progress = [0.0]
        self._task_uploaded_bytes[task_id] = 0
        self._set_runtime_task_fields(task_id, upload_chunk_done=baseline_chunks, upload_chunk_total=total_chunks)
        try:
            for full_path, rel_path, file_size in all_files:
                rel_dir = os.path.dirname(rel_path).replace("\\", "/")
                file_teldrive_path = base_teldrive_path + "/" + rel_dir if rel_dir else base_teldrive_path
                file_uploaded_before = uploaded_total[0]
                file_chunks_before = confirmed_chunks_total[0]
                file_total_chunks = self._count_file_chunks(file_size)
                async def make_progress_cb(base_uploaded, base_chunks):
                    async def progress_callback(uploaded: int, total: int, confirmed_parts: int, total_parts: int):
                        current_total = base_uploaded + uploaded
                        current_chunks = min(total_chunks, base_chunks + confirmed_parts)
                        self._task_uploaded_bytes[task_id] = current_total
                        self._set_runtime_task_fields(task_id, upload_chunk_done=current_chunks, upload_chunk_total=total_chunks, upload_note=None, upload_note_level=None)
                        self._record_upload_progress_checkpoint(task_id, {"task_id": task_id, "upload_chunk_done": current_chunks})
                        if token:
                            await db.update_upload_checkpoint(task_id, token, current_chunks, total_chunks)
                        if total_chunks > 0:
                            progress = round(current_chunks / total_chunks * 100, 1)
                        elif total_size > 0:
                            progress = round(current_total / total_size * 100, 1)
                        else:
                            progress = 100.0
                        now = time.monotonic()
                        if progress - _last_progress[0] >= 1.0 or now - _last_broadcast[0] >= 1.0 or progress >= 100.0:
                            _last_progress[0] = progress
                            _last_broadcast[0] = now
                            await db.update_task(task_id, upload_progress=progress, upload_speed="")
                            await self._broadcast_task_update(task_id)
                    return progress_callback
                cb = await make_progress_cb(file_uploaded_before, file_chunks_before)
                result = await asyncio.wait_for(teldrive.upload_file_chunked(full_path, file_teldrive_path, cb), timeout=self._calc_upload_timeout(file_size))
                self._store_upload_session_meta(task_id, result.get("upload_meta"))
                if not result.get("success"):
                    if self._is_polluted_upload_error(result.get("error")):
                        self._set_runtime_task_fields(task_id, upload_note="remote parts polluted; clean then retry", upload_note_level="warning")
                    raise Exception(f"upload failed: {rel_path} - {result.get('error', 'unknown error')}")
                uploaded_total[0] += file_size
                confirmed_chunks_total[0] = min(total_chunks, confirmed_chunks_total[0] + file_total_chunks)
                self._set_runtime_task_fields(task_id, upload_chunk_done=confirmed_chunks_total[0], upload_chunk_total=total_chunks, upload_note=None, upload_note_level=None)
                if token:
                    await db.update_upload_checkpoint(task_id, token, confirmed_chunks_total[0], total_chunks)
            self._set_runtime_task_fields(task_id, upload_chunk_done=total_chunks, upload_chunk_total=total_chunks, upload_note=None, upload_note_level=None)
            self._upload_confirmed_checkpoints[task_id] = float(total_chunks)
            if token:
                await db.complete_upload_session(task_id, token, float(total_chunks), int(total_chunks))
            self._reset_upload_retry_state(task_id)
            self._mark_upload_session_finalized(task_id)
            await db.update_task(task_id, upload_confirmed_chunks=float(total_chunks), upload_confirmed_total=int(total_chunks))
            await self._broadcast_task_update(task_id)
        finally:
            self._task_uploaded_bytes.pop(task_id, None)

    async def _upload(self, task_id: str, local_path: str, teldrive_path: str = "/", token: Optional[str] = None):
        import time
        teldrive = self._require_teldrive()
        _last_broadcast = [0.0]
        _last_progress = [0.0]
        self._task_uploaded_bytes[task_id] = 0
        file_size_on_disk = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
        total_chunks = self._count_file_chunks(file_size_on_disk)
        baseline_chunks = self._get_upload_display_baseline(task_id, total_chunks)
        self._set_runtime_task_fields(task_id, upload_chunk_done=baseline_chunks, upload_chunk_total=total_chunks)
        async def progress_callback(uploaded: int, total: int, confirmed_parts: int, total_parts: int):
            self._task_uploaded_bytes[task_id] = uploaded
            current_confirmed_parts = min(total_chunks or total_parts, baseline_chunks + confirmed_parts)
            self._set_runtime_task_fields(task_id, upload_chunk_done=current_confirmed_parts, upload_chunk_total=total_chunks or total_parts, upload_note=None, upload_note_level=None)
            self._record_upload_progress_checkpoint(task_id, {"task_id": task_id, "upload_chunk_done": current_confirmed_parts})
            if token:
                await db.update_upload_checkpoint(task_id, token, current_confirmed_parts, int(total_chunks or total_parts))
            if total_chunks > 0:
                progress = round(current_confirmed_parts / total_chunks * 100, 1)
            elif total_parts > 0:
                progress = round(current_confirmed_parts / total_parts * 100, 1)
            elif total > 0:
                progress = round(uploaded / total * 100, 1)
            else:
                progress = 100.0
            now = time.monotonic()
            if progress - _last_progress[0] >= 2.0 or now - _last_broadcast[0] >= 2.0 or progress >= 100.0:
                _last_progress[0] = progress
                _last_broadcast[0] = now
                await db.update_task(task_id, upload_progress=progress, upload_speed="")
                await self._broadcast_task_update(task_id)
        result = await asyncio.wait_for(teldrive.upload_file_chunked(local_path, teldrive_path, progress_callback), timeout=self._calc_upload_timeout(file_size_on_disk))
        try:
            self._store_upload_session_meta(task_id, result.get("upload_meta"))
            if result.get("success"):
                self._set_runtime_task_fields(task_id, upload_chunk_done=total_chunks, upload_chunk_total=total_chunks, upload_note=None, upload_note_level=None)
                self._upload_confirmed_checkpoints[task_id] = float(total_chunks)
                if token:
                    await db.complete_upload_session(task_id, token, float(total_chunks), int(total_chunks))
                self._clear_upload_retry_budget(task_id)
                self._mark_upload_session_finalized(task_id)
                await db.update_task(task_id, upload_confirmed_chunks=float(total_chunks), upload_confirmed_total=int(total_chunks))
                await self._broadcast_task_update(task_id)
            else:
                error = result.get("error", "upload failed")
                if self._is_polluted_upload_error(error):
                    self._set_runtime_task_fields(task_id, upload_note="remote parts polluted; clean then retry", upload_note_level="warning")
                raise Exception(error)
        finally:
            self._task_uploaded_bytes.pop(task_id, None)

    async def _broadcast_task_update(self, task_id: str, task_data: Optional[dict] = None):
        """广播任务状态更新（优先使用传入的 task_data 避免查库）"""
        task = self._merge_runtime_task_fields(task_data or await db.get_task(task_id))
        if task:
            await self.broadcast({
                "type": "task_update",
                "data": task
            })


    # ===========================================
    # 手动添加任务（通过面板）
    # ===========================================

    async def register_external_task(self, gid: str, url: str, filename: Optional[str] = None,
                                     teldrive_path: str = "/", status: str = "pending",
                                     aria2_options: Optional[dict] = None) -> Optional[dict]:
        """为外部提交到 aria2 的任务注册 TelDrive 目标目录。"""
        gid = str(gid or "").strip()
        if not gid:
            return None

        normalized_path = self._normalize_teldrive_path(teldrive_path)
        options_json = self._serialize_aria2_options(aria2_options)
        await db.add_task(gid, url, filename, normalized_path, options_json)
        await db.update_task(
            gid,
            status=status,
            aria2_gid=gid,
            teldrive_path=normalized_path,
            aria2_options_json=options_json,
        )
        self._known_gids.add(gid)
        await self._broadcast_task_update(gid)
        return self._merge_runtime_task_fields(await db.get_task(gid))

    async def add_task(self, url: str, filename: Optional[str] = None,
                       teldrive_path: str = "/") -> dict:
        """通过面板手动添加下载+上传任务"""
        download_dir = self.config["aria2"].get("download_dir", "./downloads")
        options = {"dir": download_dir}
        if filename:
            options["out"] = filename
        if self._is_serial_transfer_mode_enabled():
            return await self.enqueue_serial_task(
                url, filename, teldrive_path=teldrive_path, aria2_options=options
            )

        aria2 = self._require_aria2()
        gid = await aria2.add_uri(url, options)
        task = await self.register_external_task(
            gid, url, filename, teldrive_path=teldrive_path,
            status="downloading", aria2_options=options
        )
        if task:
            return task
        stored_task = await db.get_task(gid)
        if not stored_task:
            raise RuntimeError("task was added to aria2 but was not registered locally")
        return self._merge_runtime_task_fields(stored_task) or stored_task


    # ===========================================
    # 任务操作
    # ===========================================

    async def pause_task(self, task_id: str) -> dict:
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "?????"}
        status = task.get("status")
        if status not in ("downloading", "uploading", "pending"):
            return {"success": False, "message": "??????????????????"}
        try:
            if status == "uploading":
                self._cancel_existing_upload(task_id)
                self.clear_upload_progress(task_id)
                self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
                self._clear_upload_retry_budget(task_id)
                self._clear_upload_session_state(task_id, keep_meta=True)
                if task.get("upload_session_token"):
                    await db.release_upload_session(task_id, task["upload_session_token"], "idle", keep_checkpoint=True)
                old_gid = task.get("aria2_gid", "")
                if old_gid:
                    self._uploading_gids.discard(old_gid)
                    self._serial_gate_paused_gids.discard(old_gid)
                    self._serial_gate_releasing_gids.discard(old_gid)
                await db.update_task(task_id, status="paused", download_speed="", upload_speed="", error=None)
                await self._broadcast_task_update(task_id)
                return {"success": True, "message": "?????"}
            if not task.get("aria2_gid"):
                await db.update_task(task_id, status="paused", download_speed="", error=None)
                await self._broadcast_task_update(task_id)
                return {"success": True, "message": "???"}
            aria2 = self._require_aria2()
            gid = task["aria2_gid"]
            if self._is_serial_transfer_mode_enabled() and status == "pending":
                parsed = {}
                try:
                    parsed = self._parse_aria2_item(await aria2.tell_status(gid))
                except Exception:
                    pass
                try:
                    await aria2.force_remove(gid)
                except Exception:
                    try:
                        await aria2.remove(gid)
                    except Exception:
                        pass
                await self._cleanup_queued_aria2_files(task, parsed)
                self._known_gids.discard(gid)
                self._terminal_gids.add(gid)
                self._serial_gate_paused_gids.discard(gid)
                self._serial_gate_releasing_gids.discard(gid)
                await db.update_task(task_id, status="paused", aria2_gid=None, local_path=None, download_progress=0.0, download_speed="", error=None)
                await self._broadcast_task_update(task_id)
                return {"success": True, "message": "???"}
            if not self._is_serial_gate_held(gid):
                await aria2.pause(gid)
            self._serial_gate_paused_gids.discard(gid)
            self._serial_gate_releasing_gids.discard(gid)
            await db.update_task(task_id, status="paused", download_speed="")
            await self._broadcast_task_update(task_id)
            return {"success": True, "message": "???"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def resume_task(self, task_id: str) -> dict:
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "?????"}
        if task["status"] != "paused":
            return {"success": False, "message": "??????????"}
        try:
            local_path = self._get_upload_path(task.get("local_path", ""))
            if self._is_upload_stage_task(task):
                if not local_path or not os.path.exists(local_path):
                    return {"success": False, "message": "??????????????"}
                total_chunks = self._count_path_chunks(local_path)
                baseline_chunks = min(
                    self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
                    float(total_chunks) if total_chunks > 0 else self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
                )
                self._upload_confirmed_checkpoints[task_id] = baseline_chunks
                baseline_progress = round(baseline_chunks / total_chunks * 100, 1) if total_chunks > 0 else 0.0
                self._clear_upload_retry_budget(task_id)
                self._set_runtime_task_fields(task_id, upload_chunk_done=baseline_chunks, upload_chunk_total=total_chunks, upload_note="?????????????...", upload_note_level="warning")
                await db.update_task(task_id, status="uploading", upload_progress=baseline_progress, upload_speed="", error=None)
                await self._broadcast_task_update(task_id)
                self._clear_upload_session_state(task_id, keep_meta=True)
                token = uuid.uuid4().hex
                acquired = await db.try_acquire_upload_session(task_id, ("idle",), token, self._session_owner, next_state="scheduled")
                if acquired and self._mark_upload_session_scheduled(task_id):
                    self._upload_tasks[task_id] = asyncio.create_task(self._retry_upload(task_id, token))
                return {"success": True, "message": "?????"}
            if not task.get("aria2_gid"):
                await db.update_task(task_id, status="pending", download_speed="", error=None)
                await self._broadcast_task_update(task_id)
                await self._dispatch_next_serial_download()
                return {"success": True, "message": "???"}
            gid = task["aria2_gid"]
            if self._is_serial_transfer_mode_enabled():
                aria2 = self._require_aria2()
                parsed = {}
                try:
                    parsed = self._parse_aria2_item(await aria2.tell_status(gid))
                except Exception:
                    pass
                try:
                    await aria2.force_remove(gid)
                except Exception:
                    try:
                        await aria2.remove(gid)
                    except Exception:
                        pass
                await self._cleanup_queued_aria2_files(task, parsed)
                self._known_gids.discard(gid)
                self._terminal_gids.add(gid)
                self._serial_gate_paused_gids.discard(gid)
                self._serial_gate_releasing_gids.discard(gid)
                await db.update_task(task_id, status="pending", aria2_gid=None, local_path=None, download_progress=0.0, download_speed="", error=None)
                await self._dispatch_next_serial_download()
            else:
                aria2 = self._require_aria2()
                self._serial_gate_paused_gids.discard(gid)
                self._serial_gate_releasing_gids.discard(gid)
                await aria2.unpause(gid)
                await db.update_task(task_id, status="downloading")
            await self._broadcast_task_update(task_id)
            return {"success": True, "message": "???"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def cancel_task(self, task_id: str) -> dict:
        """取消任务"""
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "任务不存在"}
        if task["status"] in ("completed", "cancelled"):
            return {"success": False, "message": "任务已结束"}

        try:
            if task.get("aria2_gid"):
                try:
                    aria2 = self._require_aria2()
                    await aria2.force_remove(task["aria2_gid"])
                except Exception:
                    pass

            self._cancel_existing_upload(task_id)
            self.clear_upload_progress(task_id)
            self._clear_runtime_task_fields(task_id)
            self._reset_upload_retry_state(task_id)
            self._clear_upload_session_state(task_id)

            old_gid = task.get("aria2_gid", "")
            if old_gid:
                self._uploading_gids.discard(old_gid)
                self._known_gids.discard(old_gid)
                self._terminal_gids.discard(old_gid)
                self._serial_gate_paused_gids.discard(old_gid)
                self._serial_gate_releasing_gids.discard(old_gid)

            local = self._get_upload_path(task.get("local_path", ""))

            if local and os.path.exists(local):
                if os.path.isdir(local):
                    shutil.rmtree(local, ignore_errors=True)
                else:
                    os.remove(local)

            await db.delete_task(task_id)
            await self.broadcast({"type": "task_deleted", "data": {"task_id": task_id}})
            return {"success": True, "message": "已取消"}
        except Exception as e:

            return {"success": False, "message": str(e)}



    def _cancel_existing_upload(self, task_id: str):
        """取消正在进行的上传任务（如果有）"""
        existing_task = self._upload_tasks.pop(task_id, None)
        if existing_task and not existing_task.done():
            existing_task.cancel()
            logger.info(f"已取消任务 {task_id} 的旧上传协程")

        # 清理 _uploading_gids 中对应的 GID，解除去重锁定
        # task_id 本身可能就是 GID（直接用 GID 做 task_id 的情况）
        self._uploading_gids.discard(task_id)

    async def retry_task(self, task_id: str) -> dict:
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "?????"}
        if task["status"] not in ("failed", "uploading"):
            return {"success": False, "message": "?????????????"}
        self._cancel_existing_upload(task_id)
        self._clear_upload_session_state(task_id, keep_meta=True)
        if task.get("upload_session_token"):
            await db.release_upload_session(task_id, task["upload_session_token"], "idle", keep_checkpoint=True)
        old_gid = task.get("aria2_gid", "")
        if old_gid:
            self._uploading_gids.discard(old_gid)
            self._known_gids.discard(old_gid)
            self._terminal_gids.discard(old_gid)
            self._serial_gate_paused_gids.discard(old_gid)
            self._serial_gate_releasing_gids.discard(old_gid)
        self._clear_upload_retry_budget(task_id)
        local_path = self._get_upload_path(task.get("local_path", ""))
        if local_path and os.path.exists(local_path):
            total_chunks = self._count_path_chunks(local_path)
            baseline_chunks = min(
                self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
                float(total_chunks) if total_chunks > 0 else self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
            )
            self._upload_confirmed_checkpoints[task_id] = baseline_chunks
            baseline_progress = round(baseline_chunks / total_chunks * 100, 1) if total_chunks > 0 else 0.0
            self._set_runtime_task_fields(task_id, upload_chunk_done=baseline_chunks, upload_chunk_total=total_chunks, upload_note="?????????????...", upload_note_level="warning")
            await db.update_task(task_id, status="uploading", upload_progress=baseline_progress, upload_speed="", error=None)
            await self._broadcast_task_update(task_id)
            token = uuid.uuid4().hex
            acquired = await db.try_acquire_upload_session(task_id, ("idle",), token, self._session_owner, next_state="scheduled")
            if acquired and self._mark_upload_session_scheduled(task_id):
                self._upload_tasks[task_id] = asyncio.create_task(self._retry_upload(task_id, token))
            return {"success": True, "message": "??????"}
        aria2 = self._require_aria2()
        url = task.get("url", "")
        if not url and old_gid:
            try:
                status = await aria2.tell_status(old_gid)
                files = status.get("files", [])
                if files:
                    uris = files[0].get("uris", [])
                    if uris:
                        url = uris[0].get("uri", "")
            except Exception:
                pass
        if not url:
            return {"success": False, "message": "????????? URL ????????"}
        options = self._prepare_aria2_options(task)
        try:
            if old_gid:
                try:
                    await aria2.remove(old_gid)
                except Exception:
                    pass
            self._clear_runtime_task_fields(task_id)
            if self._is_serial_transfer_mode_enabled():
                await db.update_task(task_id, status="pending", aria2_gid=None, aria2_options_json=self._serialize_aria2_options(options), download_progress=0, upload_progress=0, download_speed="", upload_speed="", error=None, local_path=None, url=url)
                self._upload_confirmed_checkpoints[task_id] = 0.0
                await db.update_task(task_id, upload_session_state="idle", upload_session_token=None, upload_session_owner=None, upload_confirmed_chunks=0, upload_confirmed_total=0, upload_finished_at=None)
                await self._broadcast_task_update(task_id)
                await self._dispatch_next_serial_download()
                return {"success": True, "message": "?????????"}
            new_gid = await aria2.add_uri(url, options)
            await db.update_task(task_id, status="downloading", aria2_gid=new_gid, aria2_options_json=self._serialize_aria2_options(options), download_progress=0, upload_progress=0, download_speed="", upload_speed="", error=None, local_path=None, url=url)
            self._upload_confirmed_checkpoints[task_id] = 0.0
            await db.update_task(task_id, upload_session_state="idle", upload_session_token=None, upload_session_owner=None, upload_confirmed_chunks=0, upload_confirmed_total=0, upload_finished_at=None)
            self._known_gids.add(new_gid)
            await self._broadcast_task_update(task_id)
            return {"success": True, "message": "??????"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def _retry_upload(self, task_id: str, token: str):
        if not await db.confirm_upload_session_running(task_id, token):
            logger.info(f"skip duplicate retry upload runner for task {task_id}, token={token}")
            self._upload_tasks.pop(task_id, None)
            await self._refresh_upload_session_from_db(task_id)
            return
        self._upload_session_state[task_id] = "running"
        keep_session_meta = False
        finalized = False
        await self._wait_upload_slot()
        try:
            task = await self._refresh_upload_session_from_db(task_id)
            if not task:
                return
            local_path = self._get_upload_path(task.get("local_path", ""))
            if not local_path or not os.path.exists(local_path):
                self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
                await db.update_task(task_id, status="failed", error="local file missing, cannot retry upload")
                await db.release_upload_session(task_id, token, "idle", error="local file missing, cannot retry upload")
                await self._broadcast_task_update(task_id)
                return
            teldrive_path = self._get_task_teldrive_path(task, local_path)
            max_retries = self.config.get("upload", {}).get("max_retries", 3)
            retry_attempt = self._upload_retry_counts.get(task_id, 0)
            total_chunks = self._count_path_chunks(local_path)
            baseline_chunks = min(
                self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
                float(total_chunks) if total_chunks > 0 else self._coerce_upload_checkpoint_value(task.get("upload_confirmed_chunks")),
            )
            self._upload_confirmed_checkpoints[task_id] = baseline_chunks
            baseline_progress = round(baseline_chunks / total_chunks * 100, 1) if total_chunks > 0 else 0.0
            retry_note = f"retrying upload automatically ({retry_attempt}/{max_retries})..." if retry_attempt > 0 else "retrying upload..."
            self._set_runtime_task_fields(task_id, upload_chunk_done=baseline_chunks, upload_chunk_total=total_chunks, upload_note=retry_note, upload_note_level="warning")
            await db.update_task(task_id, status="uploading", upload_progress=baseline_progress, upload_speed="", error=None)
            await self._broadcast_task_update(task_id)
            if os.path.isdir(local_path):
                await self._upload_directory(task_id, local_path, teldrive_path, token)
            else:
                await self._upload(task_id, local_path, teldrive_path, token)
            await self._auto_delete_local(task_id, local_path)
            await self._check_disk_usage()
            finalized_task = await db.get_task(task_id)
            finalized = bool(finalized_task and finalized_task.get("upload_finished_at"))
        except asyncio.CancelledError:
            logger.info(f"task {task_id} retry upload cancelled")
        except Exception as e:
            logger.error(f"task {task_id} retry upload failed: {e}")
            keep_session_meta = self._is_polluted_upload_error(str(e))
            self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
            next_state = "failed_cleanup_required" if keep_session_meta else "idle"
            await db.update_task(task_id, status="failed", error=str(e), upload_session_state=next_state)
            await db.release_upload_session(task_id, token, next_state, error=str(e), keep_checkpoint=True)
            await self._broadcast_task_update(task_id)
        finally:
            self._release_upload_slot()
            self._upload_tasks.pop(task_id, None)
            if finalized:
                self._mark_upload_session_finalized(task_id)
            else:
                self._clear_upload_session_state(task_id, keep_meta=keep_session_meta)

    async def delete_task(self, task_id: str) -> dict:
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "?????"}
        if task["status"] in ("downloading", "uploading", "pending", "paused"):
            await self.cancel_task(task_id)
        gid = task.get("aria2_gid")
        if gid:
            self._known_gids.discard(gid)
            self._serial_gate_paused_gids.discard(gid)
            self._serial_gate_releasing_gids.discard(gid)
            try:
                aria2 = self._require_aria2()
                await aria2.remove(gid)
            except Exception:
                pass
        self._clear_runtime_task_fields(task_id)
        self._clear_upload_session_state(task_id)
        await db.delete_task(task_id)
        await self.broadcast({"type": "task_deleted", "data": {"task_id": task_id}})
        return {"success": True, "message": "???"}

    async def get_all_tasks(self) -> list:
        """获取所有任务"""
        tasks = await db.get_all_tasks()
        return [self._merge_runtime_task_fields(task) for task in tasks if task]

    async def get_task(self, task_id: str) -> Optional[dict]:
        """获取单个任务"""
        task = await db.get_task(task_id)
        return self._merge_runtime_task_fields(task) if task else None





# 全局单例
task_manager = TaskManager()
