"""任务管理器 - 监控 aria2 下载并自动上传到 TelDrive"""

import asyncio
import uuid
import os
import shutil
import logging
import psutil
from typing import Optional, Set
from pathlib import Path

from app.config import load_config
from app.aria2_client import Aria2Client
from app.modules.aria2teldrive.teldrive_client import TelDriveClient
from app.downloader import downloader
from app import database as db


def get_aria2_rpc_url(config: dict) -> str:
    """获取 Aria2 RPC 地址"""
    return config.get("aria2", {}).get("rpc_url", "http://localhost:6800/jsonrpc")

from pathlib import Path
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
        # 上传重试计数：task_id -> 已重试次数
        self._upload_retry_counts: dict = {}
        # 自动重试计时器
        self._last_retry_time: float = 0.0
        # 上传速度跟踪：per-task 已上传字节 → monitor loop 汇总算总速度
        self._task_uploaded_bytes: dict = {}   # task_id -> 当前已上传字节
        self._upload_total_snapshot: int = 0   # 上次快照时的总字节
        self._upload_time_snapshot: float = 0.0
        self._upload_speed: float = 0.0
        # 磁盘空间限制：通过调控并发数防止新任务占用空间
        self._disk_throttled: bool = False
        self._disk_limited_concurrent: int = 0  # 磁盘限流期间的当前并发数限制，0=未限流
        self._disk_usage_info: dict = {}  # 缓存磁盘使用信息
        self._disk_recovery_time: float = 0.0  # 上次恢复并发的时间戳，用于6s冷却
        # CPU 限速：通过限制总下载速度控制 CPU 使用率
        self._cpu_speed_limit: int = 0  # 当前速度限制(bytes/sec)，0=无限制
        self._cpu_info: dict = {}
        self._cpu_samples: list = []  # CPU 采样历史，用于滑动平均
        self._last_download_speed: int = 0  # 缓存最近的 aria2 下载速度

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

    def reload_config(self):
        """重新加载配置并重建客户端"""
        self.config = load_config()
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

    async def _apply_aria2_options(self):
        """将本地配置同步到远端 aria2"""
        try:
            cfg = self.config
            options = {
                "max-concurrent-downloads": str(cfg["aria2"].get("max_concurrent", 3)),
                "dir": cfg["aria2"].get("download_dir", "./downloads"),
            }
            await self.aria2.change_global_option(options)
            # logger.info(f"已同步 aria2 全局选项: {options}")
        except Exception as e:
            pass

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

    def _get_builtin_download_speed(self) -> int:
        total_speed = 0
        for task in downloader.get_all_tasks():
            if task.status == TaskStatus.DOWNLOADING:
                total_speed += int(task.speed or 0)
        return total_speed

    def _get_builtin_task_snapshot(self, task_id: str) -> dict:
        task = downloader.get_task(task_id)
        if not task:
            return {}
        total_size = int(task.total_size or 0)
        progress = round(task.downloaded / total_size * 100, 1) if total_size > 0 else 0
        return {
            "download_progress": progress,
            "file_size": task.to_dict()["total_str"],
            "download_speed": task.to_dict()["speed_str"] if task.status == TaskStatus.DOWNLOADING else "",
        }

    def track_upload_progress(self, task_id: str, uploaded_bytes: int):
        self._task_uploaded_bytes[task_id] = max(0, int(uploaded_bytes or 0))

    def clear_upload_progress(self, task_id: str):
        self._task_uploaded_bytes.pop(task_id, None)

    def get_global_stat(self) -> dict:
        """获取当前缓存的全局统计数据（供 WS init 立即推送）"""
        data = {
            "download_speed": self._last_download_speed + self._get_builtin_download_speed(),
            "upload_speed": int(self._upload_speed),
        }
        if self._disk_usage_info:
            data["disk"] = self._disk_usage_info
        if self._cpu_info:
            data["cpu"] = self._cpu_info
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
                # 注意顺序：先检测 CPU，再检测磁盘
                # 确保磁盘恢复并发时能感知到最新的 CPU 状态
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

                await asyncio.sleep(2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(5)

    async def _check_disk_usage(self):
        """检测磁盘使用量，通过动态调控并发数限制新任务派发"""
        max_gb = self.config.get("upload", {}).get("max_disk_usage", 0)
        max_concurrent = self.config["aria2"].get("max_concurrent", 3)

        # 始终采集磁盘信息用于仪表盘显示
        try:
            download_dir = get_download_dir(self.config)
            usage = shutil.disk_usage(download_dir)
            used_gb = round(usage.used / (1024 ** 3), 2)
            total_gb = round(usage.total / (1024 ** 3), 2)
            free_gb = round(usage.free / (1024 ** 3), 2)
            percent = round(used_gb / total_gb * 100, 1) if total_gb > 0 else 0
        except Exception as e:
            logger.debug(f"检测磁盘使用失败: {e}")
            return

        # 限流逻辑：仅在设置了上限时生效
        if max_gb <= 0:
            if self._disk_throttled:
                await self._resume_aria2_downloads()
        else:
            upper_threshold = max_gb * 0.9
            lower_threshold = max_gb * 0.6

            if used_gb >= upper_threshold and not self._disk_throttled:
                # 超上限 → 限制并发数为 1，阻止新任务派发
                try:
                    self._disk_throttled = True
                    self._disk_limited_concurrent = 1
                    await self._apply_concurrent()
                    logger.warning(
                        f"磁盘使用 {used_gb}GB >= 上限 {upper_threshold:.1f}GB，"
                        f"限制并发数为 1")
                except Exception as e:
                    logger.error(f"磁盘限流设置并发数失败: {e}")

            elif self._disk_throttled and used_gb < lower_threshold:
                # 低于下限 → 逐步恢复并发数（每次+1，冷却6秒后再检查磁盘是否仍安全）
                import time
                now = time.monotonic()
                if now - self._disk_recovery_time >= 6.0:
                    # 如果 CPU 正在限速，暂不增加并发，避免雪崩
                    if self._cpu_speed_limit > 0:
                        logger.info(f"磁盘恢复暂停：CPU 正在限速({self._cpu_speed_limit // 1024}KB/s)，"
                                    f"等待 CPU 负载降低后再增加并发")
                    elif used_gb < upper_threshold:
                        new_concurrent = min(self._disk_limited_concurrent + 1, max_concurrent)
                        try:
                            self._disk_limited_concurrent = new_concurrent
                            self._disk_recovery_time = now
                            await self._apply_concurrent()
                            logger.info(f"磁盘空间释放(已用 {used_gb}GB < 下限 {lower_threshold:.1f}GB)，"
                                        f"并发数恢复到 {new_concurrent}/{max_concurrent}")
                            if new_concurrent >= max_concurrent:
                                self._disk_throttled = False
                                self._disk_limited_concurrent = 0
                                self._disk_recovery_time = 0.0
                                logger.info("磁盘限流解除，并发数已完全恢复")
                        except Exception as e:
                            logger.error(f"磁盘限流恢复并发数失败: {e}")
                    else:
                        logger.info(f"磁盘恢复暂停：已用 {used_gb}GB 接近上限 {upper_threshold:.1f}GB")

        # 限流逻辑执行完后，构建前端显示数据（确保状态准确）
        if not self._disk_throttled:
            throttle_level = 0
        elif self._disk_limited_concurrent < max_concurrent:
            throttle_level = 1
        else:
            throttle_level = 0

        if max_gb > 0:
            display_free = round(max(0, max_gb - used_gb), 2)
            display_percent = round(used_gb / max_gb * 100, 1)
        else:
            display_free = free_gb
            display_percent = percent

        self._disk_usage_info = {
            "used_gb": used_gb,
            "total_gb": total_gb,
            "free_gb": display_free,
            "percent": display_percent,
            "limit_gb": max_gb,
            "throttled": throttle_level,
            "current_concurrent": self._disk_limited_concurrent if self._disk_throttled else max_concurrent,
            "max_concurrent": max_concurrent
        }

    def _get_effective_concurrent(self) -> int:
        """获取磁盘限流后的有效并发数，绝不超过用户设定值"""
        max_c = self.config["aria2"].get("max_concurrent", 3)
        if self._disk_throttled and self._disk_limited_concurrent > 0:
            return max(1, min(self._disk_limited_concurrent, max_c))
        return max_c

    async def _apply_concurrent(self):
        """统一设置 aria2 并发数，确保磁盘和 CPU 限流不冲突"""
        target = self._get_effective_concurrent()
        try:
            await self.aria2.change_global_option(
                {"max-concurrent-downloads": str(target)})
        except Exception as e:
            logger.error(f"设置 aria2 并发数失败: {e}")
        return target

    async def _resume_aria2_downloads(self):
        """解除磁盘限流"""
        self._disk_throttled = False
        self._disk_limited_concurrent = 0
        if self._disk_usage_info:
            self._disk_usage_info["throttled"] = 0
        target = await self._apply_concurrent()
        logger.info(f"磁盘限流解除，恢复 aria2 并发数={target}")

    async def _check_cpu_usage(self):
        """检测 CPU 使用率，通过限制总下载速度控制资源占用"""
        cpu_limit = self.config.get("upload", {}).get("cpu_limit", 85)

        # 始终采集 CPU 数据用于仪表盘显示
        try:
            raw_pct = psutil.cpu_percent(interval=None)
            self._cpu_samples.append(raw_pct)
            if len(self._cpu_samples) > 3:
                self._cpu_samples.pop(0)
            cpu_pct = sum(self._cpu_samples) / len(self._cpu_samples)
        except Exception as e:
            logger.debug(f"检测 CPU 使用失败: {e}")
            return

        if cpu_limit <= 0:
            if self._cpu_speed_limit > 0:
                self._cpu_speed_limit = 0
                try:
                    await self.aria2.change_global_option(
                        {"max-overall-download-limit": "0"})
                except Exception:
                    pass
            # 即使不限制也显示 CPU 数据
            self._cpu_info = {
                "percent": round(cpu_pct, 1),
                "limit": 0,
                "throttled": 0,
                "speed_limit": 0
            }
            return

        try:
            cpu_lower = cpu_limit * 0.75

            # 限流级别用于前端显示
            throttle_level = 0 if self._cpu_speed_limit == 0 else 1

            self._cpu_info = {
                "percent": round(cpu_pct, 1),
                "limit": cpu_limit,
                "throttled": throttle_level,
                "speed_limit": self._cpu_speed_limit
            }

            if cpu_pct >= cpu_limit:
                # CPU 超上限：降低下载速度
                if self._cpu_speed_limit == 0:
                    # 首次限速：获取当前下载速度，限制为 75%
                    try:
                        stat = await self.aria2.get_global_stat()
                        current_speed = int(stat.get("downloadSpeed", 0))
                        if current_speed > 0:
                            self._cpu_speed_limit = max(102400, int(current_speed * 0.75))  # 最低 100KB/s
                        else:
                            self._cpu_speed_limit = 1048576  # 默认限速 1MB/s
                    except Exception:
                        self._cpu_speed_limit = 1048576
                else:
                    # 持续高负载：继续降低 25%
                    self._cpu_speed_limit = max(102400, int(self._cpu_speed_limit * 0.75))
                try:
                    await self.aria2.change_global_option(
                        {"max-overall-download-limit": str(self._cpu_speed_limit)})
                    logger.info(f"CPU 限速：下载限速 {self._cpu_speed_limit // 1024}KB/s（CPU {cpu_pct:.0f}%）")
                except Exception as e:
                    logger.error(f"CPU 限速设置失败: {e}")

            elif cpu_pct < cpu_lower and self._cpu_speed_limit > 0:
                # CPU 低于下限：逐步提高限速 33%
                new_limit = int(self._cpu_speed_limit * 1.33)
                # 速度足够高时解除限制（超过 50MB/s 视为无需限速）
                if new_limit >= 52428800:
                    self._cpu_speed_limit = 0
                    speed_str = "0"
                    logger.info(f"CPU 恢复：解除下载限速（CPU {cpu_pct:.0f}%）")
                else:
                    self._cpu_speed_limit = new_limit
                    speed_str = str(new_limit)
                    logger.info(f"CPU 恢复：下载限速提升至 {new_limit // 1024}KB/s（CPU {cpu_pct:.0f}%）")
                try:
                    await self.aria2.change_global_option(
                        {"max-overall-download-limit": speed_str})
                except Exception as e:
                    logger.error(f"CPU 限速恢复失败: {e}")

        except Exception as e:
            logger.debug(f"检测 CPU 使用失败: {e}")

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
            logger.debug(f"aria2 轮询失败: {e}")
            return

        all_aria2_tasks = active + waiting + stopped

        # 获取 aria2 下载速度（供 monitor_loop 广播使用）
        try:
            stat = await self.aria2.get_global_stat()
            self._last_download_speed = int(stat.get("downloadSpeed", 0))
        except Exception:
            self._last_download_speed = 0


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
                        "complete": "completed",
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
                        download_progress=parsed["progress"],
                        download_speed=parsed["speed_str"],
                        file_size=parsed["file_size"],
                        local_path=parsed["file_path"]
                    )
                    self._known_gids.add(gid)
                    logger.info(f"发现 aria2 任务: {gid} ({parsed['filename']}) 状态={initial_status}")
                    await self._broadcast_task_update(task_id)

                    # 已入库时即标记终态
                    if initial_status in ("completed", "failed", "cancelled"):
                        self._terminal_gids.add(gid)

                    # 下载完成直接标记 completed（不再触发 TelDrive 上传，由内置引擎负责）
                    if aria2_status == "complete":
                        await db.update_task(task_id, status="completed",
                                             download_progress=100.0)
                        self._terminal_gids.add(gid)
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
            # 合并更新数据到已有 task 记录用于广播，避免再次查库
            task.update(update_data)
            await self._broadcast_task_update(task_id, task)

            # 下载完成 → 直接标记 completed（不再触发 TelDrive 上传，由内置引擎负责）
            if aria2_status == "complete" and current_status not in ("completed", "uploading"):
                await db.update_task(task_id, status="completed",
                                     download_progress=100.0)
                self._terminal_gids.add(gid)
                await self._broadcast_task_update(task_id)

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
            teldrive_path = self._calc_teldrive_path(local_path)

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
                await db.update_task(task_id, status="failed", error=error_msg)
                await self._broadcast_task_update(task_id)
                return

            await db.update_task(task_id, status="uploading",
                                 download_progress=100.0, download_speed="")
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
                self._upload_retry_counts[task_id] = retries + 1
                logger.info(f"自动重试上传任务 {task_id} ({retries+1}/{max_retries})")
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
        uploaded_total = [0]  # 已上传的总字节数
        _last_broadcast = [0.0]
        _last_progress = [0.0]

        # 注册 per-task 字节追踪
        self._task_uploaded_bytes[task_id] = 0

        logger.info(f"任务 {task_id} 检测到文件夹: {dir_path}，"
                    f"共 {len(all_files)} 个文件，总大小 {total_size} bytes，"
                    f"上传到 {base_teldrive_path}")


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

            async def make_progress_cb(base_uploaded):
                async def progress_callback(uploaded: int, total: int):
                    if total_size > 0:
                        current_total = base_uploaded + uploaded
                        self._task_uploaded_bytes[task_id] = current_total
                        progress = round(current_total / total_size * 100, 1)
                        now = time.monotonic()
                        if (progress - _last_progress[0] >= 1.0 or
                                now - _last_broadcast[0] >= 1.0 or
                                progress >= 100.0):
                            _last_progress[0] = progress
                            _last_broadcast[0] = now
                            await db.update_task(task_id, upload_progress=progress)
                            await self._broadcast_task_update(task_id)
                return progress_callback

            cb = await make_progress_cb(file_uploaded_before)

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
            logger.info(f"任务 {task_id} 文件上传成功: {rel_path}")

        # 所有文件上传完成
        self._task_uploaded_bytes.pop(task_id, None)
        await db.update_task(task_id, status="completed", upload_progress=100.0)
        await self._broadcast_task_update(task_id)
        logger.info(f"任务 {task_id} 文件夹上传完成: {dir_path}，共 {len(all_files)} 个文件")

    async def _upload(self, task_id: str, local_path: str, teldrive_path: str = "/"):
        """上传单个文件到 TelDrive"""
        import time
        _last_broadcast = [0.0]   # 上次广播时间
        _last_progress = [0.0]    # 上次广播的进度值

        # 注册 per-task 字节追踪
        self._task_uploaded_bytes[task_id] = 0

        async def progress_callback(uploaded: int, total: int):
            if total > 0:
                self._task_uploaded_bytes[task_id] = uploaded
                progress = round(uploaded / total * 100, 1)
                now = time.monotonic()

                # 节流：进度变化 ≥ 2% 或距上次超 2 秒才广播
                if (progress - _last_progress[0] >= 2.0 or
                        now - _last_broadcast[0] >= 2.0 or
                        progress >= 100.0):
                    _last_progress[0] = progress
                    _last_broadcast[0] = now
                    await db.update_task(task_id, upload_progress=progress)
                    await self._broadcast_task_update(task_id)

        # 动态超时：保底 10 分钟 + 每 GB 额外 10 分钟
        file_size_on_disk = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
        upload_timeout = self._calc_upload_timeout(file_size_on_disk)
        try:
            result = await asyncio.wait_for(
                self.teldrive.upload_file_chunked(
                    local_path, teldrive_path, progress_callback
                ),
                timeout=upload_timeout
            )
        except asyncio.TimeoutError:
            raise Exception(f"上传超时（超过 {upload_timeout}s）")

        self._task_uploaded_bytes.pop(task_id, None)  # 上传完成，移除追踪

        if result.get("success"):
            await db.update_task(task_id, status="completed",
                                 upload_progress=100.0)
            await self._broadcast_task_update(task_id)
            logger.info(f"任务 {task_id} 上传完成")
        else:
            error = result.get("error", "上传失败")
            raise Exception(error)

    async def _broadcast_task_update(self, task_id: str, task_data: dict = None):
        """广播任务状态更新（优先使用传入的 task_data 避免查库）"""
        task = task_data or await db.get_task(task_id)
        if task:
            await self.broadcast({
                "type": "task_update",
                "data": task
            })

    # ===========================================
    # 手动添加任务（通过面板）
    # ===========================================

    async def add_task(self, url: str, filename: str = None,
                       teldrive_path: str = "/") -> dict:
        """通过面板手动添加下载+上传任务"""
        download_dir = self.config["aria2"].get("download_dir", "./downloads")
        options = {"dir": download_dir}
        if filename:
            options["out"] = filename

        # 提交给 aria2
        gid = await self.aria2.add_uri(url, options)

        # 入库（用 GID 作为 task_id）
        task = await db.add_task(gid, url, filename, teldrive_path)
        await db.update_task(gid, status="downloading", aria2_gid=gid)
        self._known_gids.add(gid)

        await self._broadcast_task_update(gid)
        return await db.get_task(gid)

    # ===========================================
    # 任务操作
    # ===========================================

    async def pause_task(self, task_id: str) -> dict:
        """暂停任务"""
        task = await db.get_task(task_id)
        if not task:
            return {"success": False, "message": "任务不存在"}
        if task["status"] != "downloading":
            return {"success": False, "message": "只能暂停下载中的任务"}

        is_builtin_task = task_id.startswith("bd-") and not task.get("aria2_gid")

        try:
            update_data = {"status": "paused", "download_speed": ""}
            if is_builtin_task:
                paused = await downloader.pause(task_id)
                if not paused:
                    return {"success": False, "message": "内置下载任务当前不可暂停"}
                update_data.update(self._get_builtin_task_snapshot(task_id))
            else:
                await self.aria2.pause(task["aria2_gid"])
            await db.update_task(task_id, **update_data)
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

        is_builtin_task = task_id.startswith("bd-") and not task.get("aria2_gid")

        try:
            update_data = {"status": "downloading"}
            if is_builtin_task:
                resumed = await downloader.resume(task_id)
                if not resumed:
                    return {"success": False, "message": "内置下载任务当前不可恢复"}
                update_data.update(self._get_builtin_task_snapshot(task_id))
            else:
                await self.aria2.unpause(task["aria2_gid"])
            await db.update_task(task_id, **update_data)
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

        is_builtin_task = task_id.startswith("bd-") and not task.get("aria2_gid")

        try:
            if is_builtin_task and task["status"] == "uploading":
                return {"success": False, "message": "内置引擎上传中的任务暂不支持取消"}

            if is_builtin_task:
                await downloader.cancel(task_id)
                await downloader.remove_task(task_id)
            elif task.get("aria2_gid"):
                try:
                    await self.aria2.force_remove(task["aria2_gid"])
                except Exception:
                    pass

            self._cancel_existing_upload(task_id)
            self.clear_upload_progress(task_id)
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
                await db.update_task(task_id, status="failed",
                                     error="本地文件不存在，无法重试上传")
                await self._broadcast_task_update(task_id)
                return

            teldrive_path = self._calc_teldrive_path(local_path)

            # 重置上传状态
            await db.update_task(task_id, status="uploading",
                                 upload_progress=0.0, error=None)
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
        if task["status"] in ("downloading", "uploading", "pending"):
            await self.cancel_task(task_id)

        gid = task.get("aria2_gid")
        if gid:
            self._known_gids.discard(gid)
            # 从 aria2 移除下载记录
            try:
                await self.aria2.remove(gid)
            except Exception:
                pass

        await db.delete_task(task_id)
        await self.broadcast({"type": "task_deleted", "data": {"task_id": task_id}})
        return {"success": True, "message": "已删除"}

    async def get_all_tasks(self) -> list:
        """获取所有任务"""
        return await db.get_all_tasks()

    async def get_task(self, task_id: str) -> Optional[dict]:
        """获取单个任务"""
        return await db.get_task(task_id)


# 全局单例
task_manager = TaskManager()
