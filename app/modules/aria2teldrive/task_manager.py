"""任务管理器 - 监控 aria2 下载并自动上传到 TelDrive"""

import asyncio
import os
import shutil
import logging
import psutil
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
            upload_concurrency=cfg["teldrive"]["upload_concurrency"],
            random_chunk_name=cfg["teldrive"].get("random_chunk_name", True),
            max_retries=cfg.get("upload", {}).get("max_retries", 3)
        )

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

    def _get_user_max_concurrent_downloads(self) -> int:
        return max(1, int(self.config.get("aria2", {}).get("max_concurrent") or 3))

    def _get_disk_protection_threshold_gb(self) -> int:
        return max(1, int(self.config.get("aria2", {}).get("disk_protection_threshold_gb") or 5))

    def _get_disk_protection_threshold_bytes(self) -> int:
        return self._get_disk_protection_threshold_gb() * 1024 ** 3

    def _get_disk_protection_resume_bytes(self) -> int:
        return (self._get_disk_protection_threshold_gb() + 1) * 1024 ** 3


    def _get_effective_max_concurrent_downloads(self) -> int:
        if self._disk_protection_active:
            return max(1, int(self._disk_protection_applied_max_downloads or 1))
        return self._get_user_max_concurrent_downloads()

    async def _apply_aria2_options(self):
        """将本地配置同步到远端 aria2"""
        try:
            cfg = self.config
            aria2_cfg = cfg["aria2"]
            options = {
                "max-concurrent-downloads": str(self._get_effective_max_concurrent_downloads()),
                "split": str(aria2_cfg.get("split", 8)),
                "max-connection-per-server": str(aria2_cfg.get("max_connection_per_server", 8)),
                "min-split-size": f"{int(aria2_cfg.get('min_split_size_mb', 5))}M",
                "max-overall-download-limit": "0",
                "dir": aria2_cfg.get("download_dir", "./downloads"),
            }
            await self.aria2.change_global_option(options)
        except Exception:
            pass

    async def _sync_disk_space_download_protection(self, active_download_count: int):
        if not self.aria2:
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
                await self.aria2.change_global_option({
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
        await db.init_db()
        self._init_clients()
        # 同步配置到 aria2
        await self._apply_aria2_options()
        # 加载已有任务的 GID 到缓存
        all_tasks = await db.get_all_tasks()
        for t in all_tasks:
            if t.get("aria2_gid"):
                self._known_gids.add(t["aria2_gid"])

        # 恢复僵死的 uploading 任务（应用重启后 uploading 状态不会自动恢复）
        for t in all_tasks:

            if t["status"] == "uploading":
                task_id = t["task_id"]
                local_path = self._get_upload_path(t.get("local_path", ""))
                if local_path and os.path.exists(local_path):
                    logger.info(f"恢复僵死的上传任务: {task_id} ({t.get('filename', '?')})")
                    upload_t = asyncio.create_task(self._retry_upload(task_id))
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
        if self.aria2:
            await self.aria2.close()
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
        runtime_fields = self._runtime_task_state.get(str(merged.get("task_id") or ""))
        if runtime_fields:
            merged.update(runtime_fields)

        if str(merged.get("status") or "") in ("completed", "cancelled"):
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

        return any(marker in error for marker in blocked_markers)

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
            active = await self.aria2.tell_active() or []
            waiting = await self.aria2.tell_waiting(0, 1000) or []
            # 分页拉取所有 stopped 任务，避免超过 100 条后遗漏
            stopped = await self.aria2.tell_stopped_all() or []
        except Exception as e:
            # aria2 连接失败时静默跳过（仅每 30 秒打一次日志）
            self._last_download_speed = 0
            logger.debug(f"aria2 轮询失败: {e}")
            return

        try:
            await self._sync_disk_space_download_protection(len(active))
        except Exception as e:
            logger.debug(f"同步磁盘保护状态失败: {e}")

        all_aria2_tasks = active + waiting + stopped
        tracked_download_speed = 0


        for item in all_aria2_tasks:

            gid = item.get("gid", "")
            if not gid:
                continue

            # 终态任务不再处理，直接跳过
            if gid in self._terminal_gids:
                continue

            parsed = Aria2Client.parse_status(item)
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

                    status_map = {
                        "active": "downloading",
                        "waiting": "pending",
                        "paused": "paused",
                        "complete": "uploading",
                        "error": "failed",
                        "removed": "cancelled"
                    }
                    initial_status = status_map.get(aria2_status, "pending")

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
                        download_speed="" if aria2_status == "complete" else parsed["speed_str"],
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
                            self._upload_tasks[task_id] = asyncio.create_task(
                                self._handle_download_complete(task_id, gid)
                            )
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

            if aria2_status == "active":
                update_data["status"] = "downloading"
                tracked_download_speed += int(parsed["download_speed"] or 0)
            elif aria2_status == "waiting":

                update_data["status"] = "pending"
            elif aria2_status == "paused":
                update_data["status"] = "paused"
            elif aria2_status == "complete":
                update_data["status"] = "uploading"
                update_data["download_progress"] = 100.0
                update_data["download_speed"] = ""
            elif aria2_status == "error":
                error_code = item.get("errorCode", "")
                error_msg = item.get("errorMessage", "下载失败")
                update_data["status"] = "failed"
                update_data["error"] = f"aria2 错误 [{error_code}]: {error_msg}"
            elif aria2_status == "removed":
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
                    self._upload_tasks[task_id] = asyncio.create_task(
                        self._handle_download_complete(task_id, gid)
                    )
                else:
                    await db.update_task(task_id, status="completed")
                    self._terminal_gids.add(gid)
                    await self._broadcast_task_update(task_id)


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
            max_uploads = self.config["teldrive"].get("upload_concurrency", 4)
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

    async def _handle_download_complete(self, task_id: str, gid: str):
        """下载完成后自动上传到 TelDrive（受并发限制）"""
        if gid in self._uploading_gids:
            return
        self._uploading_gids.add(gid)

        # 等待上传槽位（动态读取并发数配置）
        await self._wait_upload_slot()
        try:
            task = await db.get_task(task_id)
            if not task or not task.get("local_path"):
                logger.warning(f"任务 {task_id} 无本地文件路径，跳过上传")
                return

            local_path = self._get_upload_path(task["local_path"])
            teldrive_path = self._get_task_teldrive_path(task, local_path)


            # 等待文件就绪（aria2 可能还在写入/移动文件）
            for attempt in range(5):
                if os.path.exists(local_path):
                    break
                logger.info(f"任务 {task_id} 等待文件就绪 ({attempt+1}/5): {local_path}")
                await asyncio.sleep(1)

            logger.info(f"[上传] 任务 {task_id}: "
                        f"local={local_path}, "
                        f"isdir={os.path.isdir(local_path)}, "
                        f"exists={os.path.exists(local_path)}, "
                        f"teldrive={teldrive_path}")

            if not os.path.exists(local_path):
                # 输出调试信息：列出下载目录内容，帮助排查路径问题
                download_dir = get_download_dir(self.config)
                try:
                    dir_contents = os.listdir(download_dir) if os.path.isdir(download_dir) else []
                    logger.error(
                        f"任务 {task_id} 文件不存在!\n"
                        f"  期望路径: {local_path}\n"
                        f"  下载目录: {download_dir}\n"
                        f"  目录存在: {os.path.isdir(download_dir)}\n"
                        f"  目录内容: {dir_contents[:20]}")
                except Exception:
                    pass
                error_msg = f"本地文件不存在: {local_path}"
                self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
                await db.update_task(task_id, status="failed", error=error_msg)
                await self._broadcast_task_update(task_id)
                return


            total_chunks = self._count_path_chunks(local_path)
            self._set_runtime_task_fields(
                task_id,
                upload_chunk_done=0,
                upload_chunk_total=total_chunks,
                upload_note=None,
                upload_note_level=None,
            )
            await db.update_task(task_id, status="uploading",
                                 download_progress=100.0, download_speed="", upload_speed="", error=None)
            await self._broadcast_task_update(task_id)


            if os.path.isdir(local_path):
                logger.info(f"[上传] 走文件夹上传: {local_path}")
                await self._upload_directory(task_id, local_path, teldrive_path)
            else:
                logger.info(f"[上传] 走单文件上传: {local_path} -> "
                            f"teldrive={teldrive_path}")
                await self._upload(task_id, local_path, teldrive_path)

            # 上传成功后清理本地文件
            await self._auto_delete_local(task_id, local_path)

        except asyncio.CancelledError:
            logger.info(f"任务 {task_id} 上传被取消（用户重试或取消）")
            # 不标记 failed，让重试逻辑接管
        except Exception as e:
            logger.error(f"任务 {task_id} 上传失败: {e}")
            self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
            await db.update_task(task_id, status="failed", error=str(e))
            await self._broadcast_task_update(task_id)

        finally:
            self._release_upload_slot()
            self._uploading_gids.discard(gid)
            self._upload_tasks.pop(task_id, None)

    async def _auto_delete_local(self, task_id: str, local_path: str):
        """上传成功后自动删除本地文件（如果配置了 auto_delete）"""
        try:
            task = await db.get_task(task_id)
            if not task or task["status"] != "completed":
                return
            if not self.config.get("upload", {}).get("auto_delete", True):
                return
            if not local_path or not os.path.exists(local_path):
                return
            # 带重试的删除（Windows 可能因句柄延迟释放而失败）
            for attempt in range(3):
                try:
                    if os.path.isdir(local_path):
                        shutil.rmtree(local_path)
                        logger.info(f"已删除本地文件夹: {local_path}")
                    else:
                        os.remove(local_path)
                        logger.info(f"已删除本地文件: {local_path}")
                    return  # 删除成功，直接返回
                except PermissionError:
                    if attempt < 2:
                        logger.warning(f"删除文件被拒绝(句柄占用)，{attempt+1}/3 次重试: {local_path}")
                        await asyncio.sleep(2)
                    else:
                        raise
        except Exception as e:
            logger.warning(f"删除本地文件失败: {local_path}, {e}")

    async def _cleanup_completed_files(self):
        """定期清理已完成任务的本地残留文件（兜底机制）"""
        if not self.config.get("upload", {}).get("auto_delete", True):
            return
        try:
            all_tasks = await db.get_all_tasks()
            max_retries = self.config.get("upload", {}).get("max_retries", 3)
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
                    logger.info(f"兜底清理成功: {local_path}")
                except Exception as e:
                    logger.warning(f"兜底清理失败: {local_path}, {e}")
        except Exception as e:
            logger.debug(f"清理文件异常: {e}")

    async def _auto_retry_failed_uploads(self):
        """自动重试失败的上传任务，超过 max_retries 次后放弃并清理本地文件"""
        max_retries = self.config.get("upload", {}).get("max_retries", 3)
        try:
            all_tasks = await db.get_all_tasks()
            for task in all_tasks:
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

                retries = self._upload_retry_counts.get(task_id, 0)
                if retries >= max_retries:
                    # 重试耗尽，跳过（文件清理由 _cleanup_completed_files 处理）
                    continue

                # 发起重试
                next_retry = retries + 1
                self._upload_retry_counts[task_id] = next_retry
                total_chunks = self._count_path_chunks(local_path)
                retry_message = f"上传失败，正在自动重试（{next_retry}/{max_retries}），等待上传槽位..."
                self._set_runtime_task_fields(
                    task_id,
                    upload_chunk_done=0,
                    upload_chunk_total=total_chunks,
                    upload_note=retry_message,
                    upload_note_level="warning",
                )
                await db.update_task(task_id, status="uploading", upload_progress=0.0, upload_speed="", error=None)
                await self._broadcast_task_update(task_id)
                logger.info(f"自动重试上传任务 {task_id} ({next_retry}/{max_retries})")
                t = asyncio.create_task(self._retry_upload(task_id))
                self._upload_tasks[task_id] = t



        except Exception as e:
            logger.debug(f"自动重试扫描异常: {e}")

    # ===========================================
    # 上传
    # ===========================================

    async def _upload_directory(self, task_id: str, dir_path: str, teldrive_path: str = "/"):
        """递归上传文件夹到 TelDrive，保留目录结构"""
        import time

        # 收集所有文件及其大小
        # 直接使用 teldrive_path 作为基础路径，不额外嵌套文件夹名
        base_teldrive_path = teldrive_path.rstrip("/") if teldrive_path != "/" else "/"
        all_files = []
        for root, _dirs, filenames in os.walk(dir_path):
            for fname in filenames:
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, dir_path)
                file_size = os.path.getsize(full_path)
                all_files.append((full_path, rel_path, file_size))

        if not all_files:
            logger.warning(f"任务 {task_id} 文件夹为空: {dir_path}")
            await db.update_task(task_id, status="completed", upload_progress=100.0)
            await self._broadcast_task_update(task_id)
            return

        total_size = sum(s for _, _, s in all_files)
        total_chunks = sum(self._count_file_chunks(s) for _, _, s in all_files)
        uploaded_total = [0]  # 已上传的总字节数
        confirmed_chunks_total = [0]
        _last_broadcast = [0.0]
        _last_progress = [0.0]

        # 注册 per-task 字节追踪
        self._task_uploaded_bytes[task_id] = 0
        self._set_runtime_task_fields(
            task_id,
            upload_chunk_done=0,
            upload_chunk_total=total_chunks,
        )

        logger.info(f"任务 {task_id} 检测到文件夹: {dir_path}，"
                    f"共 {len(all_files)} 个文件，总大小 {total_size} bytes，"
                    f"总分块 {total_chunks}，上传到 {base_teldrive_path}")

        try:
            for idx, (full_path, rel_path, file_size) in enumerate(all_files, 1):
                # 计算该文件在 TelDrive 上的目标路径
                rel_dir = os.path.dirname(rel_path).replace("\\", "/")
                if rel_dir:
                    file_teldrive_path = base_teldrive_path + "/" + rel_dir
                else:
                    file_teldrive_path = base_teldrive_path

                logger.info(f"任务 {task_id} 上传文件 [{idx}/{len(all_files)}]: "
                            f"{rel_path} -> {file_teldrive_path}")

                # 为每个文件创建进度回调（汇总到整体进度）
                file_uploaded_before = uploaded_total[0]
                file_chunks_before = confirmed_chunks_total[0]
                file_total_chunks = self._count_file_chunks(file_size)

                async def make_progress_cb(base_uploaded, base_chunks):
                    async def progress_callback(uploaded: int, total: int, confirmed_parts: int, total_parts: int):
                        current_total = base_uploaded + uploaded
                        current_chunks = base_chunks + confirmed_parts
                        self._task_uploaded_bytes[task_id] = current_total
                        self._set_runtime_task_fields(
                            task_id,
                            upload_chunk_done=current_chunks,
                            upload_chunk_total=total_chunks,
                            upload_note=None,
                            upload_note_level=None,
                        )

                        if total_chunks > 0:
                            progress = round(current_chunks / total_chunks * 100, 1)
                        elif total_size > 0:
                            progress = round(current_total / total_size * 100, 1)
                        else:
                            progress = 100.0

                        now = time.monotonic()
                        if (progress - _last_progress[0] >= 1.0 or
                                now - _last_broadcast[0] >= 1.0 or
                                progress >= 100.0):
                            _last_progress[0] = progress
                            _last_broadcast[0] = now
                            await db.update_task(task_id, upload_progress=progress, upload_speed="")
                            await self._broadcast_task_update(task_id)
                    return progress_callback

                cb = await make_progress_cb(file_uploaded_before, file_chunks_before)

                try:
                    result = await asyncio.wait_for(
                        self.teldrive.upload_file_chunked(
                            full_path, file_teldrive_path, cb
                        ),
                        timeout=self._calc_upload_timeout(file_size)
                    )
                except asyncio.TimeoutError:
                    raise Exception(f"上传超时: {rel_path}")

                if not result.get("success"):
                    raise Exception(f"上传失败: {rel_path} - {result.get('error', '未知错误')}")

                uploaded_total[0] += file_size
                confirmed_chunks_total[0] += file_total_chunks
                self._set_runtime_task_fields(
                    task_id,
                    upload_chunk_done=confirmed_chunks_total[0],
                    upload_chunk_total=total_chunks,
                    upload_note=None,
                    upload_note_level=None,
                )
                logger.info(f"任务 {task_id} 文件上传成功: {rel_path}")

            self._set_runtime_task_fields(
                task_id,
                upload_chunk_done=total_chunks,
                upload_chunk_total=total_chunks,
                upload_note=None,
                upload_note_level=None,
            )
            await db.update_task(task_id, status="completed", upload_progress=100.0, upload_speed="")
            await self._broadcast_task_update(task_id)
            logger.info(f"任务 {task_id} 文件夹上传完成: {dir_path}，共 {len(all_files)} 个文件")
        finally:
            self._task_uploaded_bytes.pop(task_id, None)



    async def _upload(self, task_id: str, local_path: str, teldrive_path: str = "/"):
        """上传单个文件到 TelDrive"""
        import time
        _last_broadcast = [0.0]   # 上次广播时间
        _last_progress = [0.0]    # 上次广播的进度值

        # 注册 per-task 字节追踪
        self._task_uploaded_bytes[task_id] = 0

        # 动态超时：保底 10 分钟 + 每 GB 额外 10 分钟
        file_size_on_disk = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
        total_chunks = self._count_file_chunks(file_size_on_disk)
        self._set_runtime_task_fields(
            task_id,
            upload_chunk_done=0,
            upload_chunk_total=total_chunks,
        )

        async def progress_callback(uploaded: int, total: int, confirmed_parts: int, total_parts: int):
            self._task_uploaded_bytes[task_id] = uploaded
            self._set_runtime_task_fields(
                task_id,
                upload_chunk_done=confirmed_parts,
                upload_chunk_total=total_parts,
                upload_note=None,
                upload_note_level=None,
            )

            if total_parts > 0:
                progress = round(confirmed_parts / total_parts * 100, 1)
            elif total > 0:
                progress = round(uploaded / total * 100, 1)
            else:
                progress = 100.0
            now = time.monotonic()

            # 节流：进度变化 ≥ 2% 或距上次超 2 秒才广播
            if (progress - _last_progress[0] >= 2.0 or
                    now - _last_broadcast[0] >= 2.0 or
                    progress >= 100.0):
                _last_progress[0] = progress
                _last_broadcast[0] = now
                await db.update_task(task_id, upload_progress=progress, upload_speed="")
                await self._broadcast_task_update(task_id)

        upload_timeout = self._calc_upload_timeout(file_size_on_disk)
        try:
            try:
                result = await asyncio.wait_for(
                    self.teldrive.upload_file_chunked(
                        local_path, teldrive_path, progress_callback
                    ),
                    timeout=upload_timeout
                )
            except asyncio.TimeoutError:
                raise Exception(f"上传超时（超过 {upload_timeout}s）")

            if result.get("success"):
                self._set_runtime_task_fields(
                    task_id,
                    upload_chunk_done=total_chunks,
                    upload_chunk_total=total_chunks,
                    upload_note=None,
                    upload_note_level=None,
                )
                await db.update_task(task_id, status="completed",
                                     upload_progress=100.0, upload_speed="")
                await self._broadcast_task_update(task_id)
                logger.info(f"任务 {task_id} 上传完成")
            else:
                error = result.get("error", "上传失败")
                raise Exception(error)
        finally:
            self._task_uploaded_bytes.pop(task_id, None)



    async def _broadcast_task_update(self, task_id: str, task_data: dict = None):
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

    async def register_external_task(self, gid: str, url: str, filename: str = None,
                                     teldrive_path: str = "/", status: str = "pending") -> Optional[dict]:
        """为外部提交到 aria2 的任务注册 TelDrive 目标目录。"""
        gid = str(gid or "").strip()
        if not gid:
            return None

        normalized_path = self._normalize_teldrive_path(teldrive_path)
        await db.add_task(gid, url, filename, normalized_path)
        await db.update_task(gid, status=status, aria2_gid=gid, teldrive_path=normalized_path)
        self._known_gids.add(gid)
        await self._broadcast_task_update(gid)
        return await db.get_task(gid)

    async def add_task(self, url: str, filename: str = None,
                       teldrive_path: str = "/") -> dict:
        """通过面板手动添加下载+上传任务"""
        download_dir = self.config["aria2"].get("download_dir", "./downloads")
        options = {"dir": download_dir}
        if filename:
            options["out"] = filename

        gid = await self.aria2.add_uri(url, options)
        task = await self.register_external_task(
            gid, url, filename, teldrive_path=teldrive_path, status="downloading"
        )
        return task or await db.get_task(gid)


    # ===========================================
    # 任务操作
    # ===========================================

    async def pause_task(self, task_id: str) -> dict:
        """暂停任务"""
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "任务不存在"}

        status = task.get("status")
        if status not in ("downloading", "uploading", "pending"):
            return {"success": False, "message": "只能暂停下载中、等待中或上传中的任务"}

        try:
            if status == "uploading":
                self._cancel_existing_upload(task_id)
                self.clear_upload_progress(task_id)
                self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
                old_gid = task.get("aria2_gid", "")
                if old_gid:
                    self._uploading_gids.discard(old_gid)
                await db.update_task(task_id, status="paused", download_speed="", upload_speed="", error=None)
                await self._broadcast_task_update(task_id)
                return {"success": True, "message": "已暂停上传"}

            if not task.get("aria2_gid"):
                return {"success": False, "message": "缺少 aria2 GID，无法暂停"}

            await self.aria2.pause(task["aria2_gid"])
            await db.update_task(task_id, status="paused", download_speed="")
            await self._broadcast_task_update(task_id)
            return {"success": True, "message": "已暂停"}
        except Exception as e:
            return {"success": False, "message": str(e)}



    async def resume_task(self, task_id: str) -> dict:
        """恢复任务"""
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "任务不存在"}
        if task["status"] != "paused":
            return {"success": False, "message": "只能恢复已暂停的任务"}

        try:
            local_path = self._get_upload_path(task.get("local_path", ""))
            if self._is_upload_stage_task(task):
                if not local_path or not os.path.exists(local_path):
                    return {"success": False, "message": "本地文件不存在，无法继续上传"}

                self._upload_retry_counts.pop(task_id, None)
                total_chunks = self._count_path_chunks(local_path)
                self._set_runtime_task_fields(
                    task_id,
                    upload_chunk_done=0,
                    upload_chunk_total=total_chunks,
                    upload_note="正在继续上传，等待上传槽位...",
                    upload_note_level="warning",
                )
                await db.update_task(task_id, status="uploading", upload_progress=0.0, upload_speed="", error=None)
                await self._broadcast_task_update(task_id)
                self._upload_tasks[task_id] = asyncio.create_task(self._retry_upload(task_id))
                return {"success": True, "message": "已恢复上传"}

            if not task.get("aria2_gid"):
                return {"success": False, "message": "缺少 aria2 GID，无法恢复"}

            await self.aria2.unpause(task["aria2_gid"])
            await db.update_task(task_id, status="downloading")
            await self._broadcast_task_update(task_id)
            return {"success": True, "message": "已恢复"}
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
                    await self.aria2.force_remove(task["aria2_gid"])
                except Exception:
                    pass

            self._cancel_existing_upload(task_id)
            self.clear_upload_progress(task_id)
            self._clear_runtime_task_fields(task_id)
            self._upload_retry_counts.pop(task_id, None)

            old_gid = task.get("aria2_gid", "")
            if old_gid:
                self._uploading_gids.discard(old_gid)
                self._known_gids.discard(old_gid)
                self._terminal_gids.discard(old_gid)

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
        """重试失败/卡住的任务"""
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "任务不存在"}
        if task["status"] not in ("failed", "uploading"):
            return {"success": False, "message": "只能重试失败或上传中的任务"}

        # 取消正在卡住的旧上传协程，清理 GID 去重标记
        self._cancel_existing_upload(task_id)
        # 也清理 aria2_gid 对应的 uploading_gids 标记
        old_gid = task.get("aria2_gid", "")
        if old_gid:
            self._uploading_gids.discard(old_gid)
            self._known_gids.discard(old_gid)
            self._terminal_gids.discard(old_gid)

        # 清除重试计数
        self._upload_retry_counts.pop(task_id, None)

        # 如果本地文件/文件夹已存在，直接重试上传
        local_path = self._get_upload_path(task.get("local_path", ""))
        if local_path and os.path.exists(local_path):
            total_chunks = self._count_path_chunks(local_path)
            self._set_runtime_task_fields(
                task_id,
                upload_chunk_done=0,
                upload_chunk_total=total_chunks,
                upload_note="正在重新上传，等待上传槽位...",
                upload_note_level="warning",
            )
            await db.update_task(task_id, status="uploading", upload_progress=0.0, upload_speed="", error=None)
            await self._broadcast_task_update(task_id)
            t = asyncio.create_task(self._retry_upload(task_id))
            self._upload_tasks[task_id] = t
            return {"success": True, "message": "正在重试上传"}


        # 否则需要重新下载
        url = task.get("url", "")

        # 如果数据库中没有 URL，尝试从 aria2 查询原始 URI
        if not url and old_gid:
            try:
                status = await self.aria2.tell_status(old_gid)
                files = status.get("files", [])
                if files:
                    uris = files[0].get("uris", [])
                    if uris:
                        url = uris[0].get("uri", "")
            except Exception:
                pass

        if not url:
            return {"success": False, "message": "无法重试：缺少下载 URL 且本地文件不存在"}

        download_dir = self.config["aria2"].get("download_dir", "./downloads")
        options = {"dir": download_dir}
        if task.get("filename"):
            options["out"] = task["filename"]

        try:
            # 先尝试从 aria2 移除旧的失败任务
            if old_gid:
                try:
                    await self.aria2.remove(old_gid)
                except Exception:
                    pass

            new_gid = await self.aria2.add_uri(url, options)
            self._clear_runtime_task_fields(task_id)
            await db.update_task(
                task_id, status="downloading", aria2_gid=new_gid,
                download_progress=0, upload_progress=0,
                download_speed="", upload_speed="",
                error=None, local_path=None, url=url
            )
            self._known_gids.add(new_gid)

            await self._broadcast_task_update(task_id)
            return {"success": True, "message": "正在重新下载"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def _retry_upload(self, task_id: str):
        """仅重试上传步骤（受并发限制）"""
        await self._wait_upload_slot()
        try:
            task = await db.get_task(task_id)
            if not task:
                return

            local_path = self._get_upload_path(task.get("local_path", ""))

            if not local_path or not os.path.exists(local_path):
                self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
                await db.update_task(task_id, status="failed",
                                     error="本地文件不存在，无法重试上传")
                await self._broadcast_task_update(task_id)
                return


            teldrive_path = self._get_task_teldrive_path(task, local_path)

            max_retries = self.config.get("upload", {}).get("max_retries", 3)
            retry_attempt = self._upload_retry_counts.get(task_id, 0)
            total_chunks = self._count_path_chunks(local_path)
            retry_note = (
                f"正在自动重试上传（{retry_attempt}/{max_retries}）..."
                if retry_attempt > 0 else
                "正在重新上传..."
            )

            # 重置上传状态
            self._set_runtime_task_fields(
                task_id,
                upload_chunk_done=0,
                upload_chunk_total=total_chunks,
                upload_note=retry_note,
                upload_note_level="warning",
            )
            await db.update_task(task_id, status="uploading",
                                 upload_progress=0.0, upload_speed="", error=None)
            await self._broadcast_task_update(task_id)


            # 判断是文件夹还是单文件
            if os.path.isdir(local_path):
                await self._upload_directory(task_id, local_path, teldrive_path)
            else:
                await self._upload(task_id, local_path, teldrive_path)

            # 上传成功后清理本地文件
            await self._auto_delete_local(task_id, local_path)

        except asyncio.CancelledError:
            logger.info(f"任务 {task_id} 重试上传被取消")
        except Exception as e:
            logger.error(f"任务 {task_id} 重试上传失败: {e}")
            self._set_runtime_task_fields(task_id, upload_note=None, upload_note_level=None)
            await db.update_task(task_id, status="failed", error=str(e))
            await self._broadcast_task_update(task_id)

        finally:
            self._release_upload_slot()
            self._upload_tasks.pop(task_id, None)

    async def delete_task(self, task_id: str) -> dict:
        """删除任务记录"""
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "任务不存在"}

        # 如果任务还在进行中，先取消
        if task["status"] in ("downloading", "uploading", "pending", "paused"):
            await self.cancel_task(task_id)


        gid = task.get("aria2_gid")
        if gid:
            self._known_gids.discard(gid)
            # 从 aria2 移除下载记录
            try:
                await self.aria2.remove(gid)
            except Exception:
                pass

        self._clear_runtime_task_fields(task_id)
        await db.delete_task(task_id)
        await self.broadcast({"type": "task_deleted", "data": {"task_id": task_id}})
        return {"success": True, "message": "已删除"}


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
