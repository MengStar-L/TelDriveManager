"""内置多连接 HTTP 下载器

替代 Aria2 作为主力下载通道：
- 多连接并行下载（类似 aria2 -x8）
- fallocate 预分配磁盘空间
- 暂停 / 恢复（断点续传）
- 实时进度追踪（速度、ETA）
- 失败 / 取消自动清理临时文件
"""

import asyncio
import logging
import os
import platform
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional, Awaitable

import aiohttp

logger = logging.getLogger(__name__)

# 固定下载目录：项目根目录 / downloads
DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

STALL_READ_TIMEOUT = 20
CHUNK_MAX_RETRIES = 4


class TaskStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UPLOADING = "uploading"


@dataclass
class ChunkState:
    """单个分块的下载状态"""
    index: int
    start: int          # 文件中的起始字节
    end: int            # 文件中的结束字节（含）
    downloaded: int = 0 # 该分块已下载字节数
    done: bool = False


@dataclass
class DownloadTask:
    """单个下载任务的完整状态"""
    task_id: str
    url: str
    filename: str
    dest_path: str              # 最终文件路径
    temp_path: str              # 下载中的临时路径 (.downloading)
    total_size: int = 0
    downloaded: int = 0
    speed: float = 0.0          # bytes/sec
    eta: float = 0.0            # 预计剩余秒数
    status: TaskStatus = TaskStatus.PENDING
    connections: int = 0        # 活跃连接数
    max_connections: int = 8
    error: str = ""
    chunks: List[ChunkState] = field(default_factory=list)
    supports_range: bool = False
    # 内部控制
    _cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    _pause_event: asyncio.Event = field(default_factory=lambda: _make_set_event())
    _task_handle: Optional[asyncio.Task] = field(default=None, repr=False)
    # 速度计算
    _speed_samples: List[float] = field(default_factory=list)
    _last_bytes: int = 0
    _last_time: float = 0.0
    # 回调
    _on_complete: Optional[Callable] = field(default=None, repr=False)
    _on_progress: Optional[Callable] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "url": self.url,
            "filename": self.filename,
            "dest_path": self.dest_path,
            "total_size": self.total_size,
            "downloaded": self.downloaded,
            "speed": self.speed,
            "eta": self.eta,
            "status": self.status.value,
            "connections": self.connections,
            "max_connections": self.max_connections,
            "error": self.error,
            "progress": round(self.downloaded / self.total_size * 100, 1) if self.total_size > 0 else 0,
            "total_str": _format_size(self.total_size),
            "downloaded_str": _format_size(self.downloaded),
            "speed_str": _format_speed(self.speed),
            "eta_str": _format_eta(self.eta),
        }


def _make_set_event() -> asyncio.Event:
    """创建一个默认已 set 的 Event（未暂停状态）"""
    e = asyncio.Event()
    e.set()
    return e


class BuiltinDownloader:
    """内置多连接 HTTP 下载器"""

    def __init__(self, max_concurrent: int = 3, connections_per_task: int = 8):
        self.max_concurrent = max_concurrent
        self.connections_per_task = connections_per_task
        self._tasks: Dict[str, DownloadTask] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=120)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def update_config(self, max_concurrent: int = 3, connections_per_task: int = 8):
        """热更新配置"""
        self.max_concurrent = max_concurrent
        self.connections_per_task = connections_per_task
        # 重建信号量（仅影响新任务）
        self._semaphore = asyncio.Semaphore(max_concurrent)

    # ─── 公开 API ───

    async def add_task(
        self,
        url: str,
        filename: str,
        task_id: str = "",
        on_complete: Optional[Callable] = None,
        on_progress: Optional[Callable] = None,
        base_dir: Optional[str] = None,
    ) -> DownloadTask:
        """添加下载任务"""
        if not task_id:
            task_id = uuid.uuid4().hex[:12]

        dest_dir = Path(base_dir) if base_dir else DOWNLOAD_DIR
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = str(dest_dir / filename)
        temp_path = dest_path + ".downloading"

        task = DownloadTask(
            task_id=task_id,
            url=url,
            filename=filename,
            dest_path=dest_path,
            temp_path=temp_path,
            max_connections=self.connections_per_task,
            _on_complete=on_complete,
            _on_progress=on_progress,
        )
        self._tasks[task_id] = task

        # 启动下载协程
        task._task_handle = asyncio.create_task(self._run_task(task))
        logger.info(f"[下载器] 添加任务: {task_id} ({filename})")
        return task

    async def pause(self, task_id: str) -> bool:
        """暂停下载任务"""
        task = self._tasks.get(task_id)
        if not task or task.status != TaskStatus.DOWNLOADING:
            return False
        task._pause_event.clear()  # 阻塞所有分块协程
        task.status = TaskStatus.PAUSED
        task.speed = 0
        task.eta = 0
        logger.info(f"[下载器] 暂停: {task_id}")
        return True

    async def resume(self, task_id: str) -> bool:
        """恢复下载任务"""
        task = self._tasks.get(task_id)
        if not task or task.status != TaskStatus.PAUSED:
            return False
        task.status = TaskStatus.DOWNLOADING
        task._pause_event.set()  # 唤醒所有分块协程
        logger.info(f"[下载器] 恢复: {task_id}")
        return True

    async def cancel(self, task_id: str) -> bool:
        """取消下载任务并清理文件"""
        task = self._tasks.get(task_id)
        if not task:
            return False
        if task.status in (TaskStatus.COMPLETED, TaskStatus.CANCELLED):
            return False
        task._cancel_event.set()
        task._pause_event.set()  # 确保暂停的协程也能退出
        task.status = TaskStatus.CANCELLED
        # 等待协程结束
        if task._task_handle and not task._task_handle.done():
            task._task_handle.cancel()
            try:
                await task._task_handle
            except (asyncio.CancelledError, Exception):
                pass
        self._cleanup_file(task)
        logger.info(f"[下载器] 取消: {task_id}")
        return True

    def get_task(self, task_id: str) -> Optional[DownloadTask]:
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> List[DownloadTask]:
        return list(self._tasks.values())

    async def remove_task(self, task_id: str):
        """移除任务记录（先取消再移除）"""
        await self.cancel(task_id)
        self._tasks.pop(task_id, None)

    def forget_task(self, task_id: str):
        """仅从内存中移除任务，不触碰磁盘文件。"""
        self._tasks.pop(task_id, None)

    async def _notify_complete(self, task: DownloadTask):
        if task._on_complete:
            try:
                await task._on_complete(task)
            except Exception as e:
                logger.error(f"[下载器] 完成回调异常: {e}")

    # ─── 内部实现 ───

    async def _run_task(self, task: DownloadTask):
        """受并发信号量控制的任务入口"""
        async with self._semaphore:
            try:
                await self._download_file(task)
            except asyncio.CancelledError:
                if task.status not in (TaskStatus.CANCELLED, TaskStatus.COMPLETED):
                    task.status = TaskStatus.CANCELLED
                    self._cleanup_file(task)
                await self._notify_complete(task)
            except Exception as e:
                logger.error(f"[下载器] 任务 {task.task_id} 失败: {e}")
                task.status = TaskStatus.FAILED
                task.error = str(e)
                task.speed = 0
                self._cleanup_file(task)
                if task._on_progress:
                    try:
                        await task._on_progress(task)
                    except Exception:
                        pass
                await self._notify_complete(task)

    async def _download_file(self, task: DownloadTask):
        """主下载逻辑"""
        session = await self._get_session()

        # 1. 探测文件信息
        async with session.head(task.url, allow_redirects=True) as resp:
            task.total_size = int(resp.headers.get("Content-Length", 0))
            accept_ranges = resp.headers.get("Accept-Ranges", "")
            task.supports_range = accept_ranges.lower() == "bytes" and task.total_size > 0

        if task.total_size == 0:
            # 无法获取大小，尝试 GET 获取
            async with session.get(task.url, allow_redirects=True) as resp:
                task.total_size = int(resp.headers.get("Content-Length", 0))
                task.supports_range = False  # 降级单连接

        logger.info(f"[下载器] {task.filename}: size={_format_size(task.total_size)}, "
                    f"range={'✓' if task.supports_range else '✗'}")

        # 2. 预分配磁盘空间
        if task.total_size > 0:
            _fallocate(task.temp_path, task.total_size)

        # 3. 设置分块
        if task.supports_range and task.total_size > 0:
            num_chunks = min(task.max_connections, max(1, task.total_size // (1024 * 1024)))
            chunk_size = task.total_size // num_chunks
            task.chunks = []
            for i in range(num_chunks):
                start = i * chunk_size
                end = (i + 1) * chunk_size - 1 if i < num_chunks - 1 else task.total_size - 1
                task.chunks.append(ChunkState(index=i, start=start, end=end))
        else:
            # 单连接
            task.chunks = [ChunkState(index=0, start=0, end=task.total_size - 1 if task.total_size > 0 else 0)]

        # 恢复已下载的偏移量（暂停恢复场景）
        # chunks 中的 downloaded 字段记录了断点

        # 4. 开始下载
        task.status = TaskStatus.DOWNLOADING
        task._last_time = time.monotonic()
        task._last_bytes = 0

        # 启动进度追踪
        progress_task = asyncio.create_task(self._progress_tracker(task))

        try:
            if len(task.chunks) == 1:
                # 单连接下载
                await self._download_single(task, session)
            else:
                # 多连接并行下载
                await self._download_multi(task, session)
        finally:
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

        # 5. 检查是否被取消
        if task._cancel_event.is_set() or task.status == TaskStatus.CANCELLED:
            return

        # 6. 重命名临时文件
        if task.status == TaskStatus.PAUSED:
            return  # 暂停状态不重命名

        if os.path.exists(task.temp_path):
            # 如果目标文件已存在，先删除
            if os.path.exists(task.dest_path):
                os.remove(task.dest_path)
            os.rename(task.temp_path, task.dest_path)

        task.status = TaskStatus.COMPLETED
        task.speed = 0
        task.downloaded = task.total_size
        logger.info(f"[下载器] 完成: {task.filename} ({_format_size(task.total_size)})")

        # 触发完成回调
        if task._on_progress:
            try:
                await task._on_progress(task)
            except Exception:
                pass
        await self._notify_complete(task)

    async def _download_single(self, task: DownloadTask, session: aiohttp.ClientSession):
        """单连接下载（不支持 Range 或小文件）"""
        chunk_state = task.chunks[0]
        task.connections = 1

        with open(task.temp_path, "ab" if chunk_state.downloaded > 0 else "wb") as f:
            if chunk_state.downloaded > 0:
                f.seek(chunk_state.downloaded)

            headers = {}
            if chunk_state.downloaded > 0 and task.supports_range:
                headers["Range"] = f"bytes={chunk_state.downloaded}-"

            async with session.get(task.url, headers=headers, allow_redirects=True) as resp:
                if resp.status not in (200, 206):
                    raise Exception(f"HTTP {resp.status}")
                while True:
                    if task._cancel_event.is_set():
                        return
                    if not task._pause_event.is_set():
                        task.connections = 0
                        await task._pause_event.wait()
                        if task._cancel_event.is_set():
                            return
                        task.connections = 1
                    try:
                        data = await asyncio.wait_for(resp.content.read(65536), timeout=STALL_READ_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise Exception(f"单连接超过 {STALL_READ_TIMEOUT}s 无数据")
                    if not data:
                        break
                    f.write(data)
                    n = len(data)
                    chunk_state.downloaded += n
                    task.downloaded += n

        chunk_state.done = True
        task.connections = 0

    async def _download_multi(self, task: DownloadTask, session: aiohttp.ClientSession):
        """多连接并行下载，支持分块级自动重试"""
        async def _worker(chunk: ChunkState):
            """单个分块的下载协程"""
            expected_size = chunk.end - chunk.start + 1
            last_error: Optional[Exception] = None

            for attempt in range(1, CHUNK_MAX_RETRIES + 1):
                current_pos = chunk.start + chunk.downloaded
                if current_pos > chunk.end or chunk.downloaded >= expected_size:
                    chunk.done = True
                    return

                headers = {"Range": f"bytes={current_pos}-{chunk.end}"}

                try:
                    task.connections += 1
                    async with session.get(task.url, headers=headers, allow_redirects=True) as resp:
                        if resp.status != 206:
                            raise Exception(f"分块 {chunk.index + 1} 收到异常状态码 {resp.status}")

                        content_range = resp.headers.get("Content-Range", "")
                        if not content_range.startswith(f"bytes {current_pos}-"):
                            raise Exception(f"分块 {chunk.index + 1} Content-Range 异常: {content_range or 'missing'}")

                        with open(task.temp_path, "r+b") as f:
                            f.seek(current_pos)
                            while True:
                                if task._cancel_event.is_set():
                                    return
                                if not task._pause_event.is_set():
                                    task.connections = max(0, task.connections - 1)
                                    await task._pause_event.wait()
                                    if task._cancel_event.is_set():
                                        return
                                    task.connections += 1
                                try:
                                    data = await asyncio.wait_for(resp.content.read(65536), timeout=STALL_READ_TIMEOUT)
                                except asyncio.TimeoutError:
                                    raise Exception(f"分块 {chunk.index + 1} 超过 {STALL_READ_TIMEOUT}s 无数据")
                                if not data:
                                    break
                                f.write(data)
                                n = len(data)
                                chunk.downloaded += n
                                task.downloaded += n

                    if chunk.downloaded < expected_size:
                        raise Exception(
                            f"分块 {chunk.index + 1} 下载不完整: {chunk.downloaded}/{expected_size} bytes"
                        )

                    chunk.done = True
                    return
                except Exception as exc:
                    last_error = exc
                    if task._cancel_event.is_set():
                        raise
                    if attempt >= CHUNK_MAX_RETRIES:
                        raise
                    wait_seconds = min(2 ** (attempt - 1), 5)
                    logger.warning(
                        f"[下载器] 分块 {chunk.index + 1}/{len(task.chunks)} 失败: {exc}，"
                        f"{wait_seconds}s 后第 {attempt} 次重试"
                    )
                    await asyncio.sleep(wait_seconds)
                finally:
                    task.connections = max(0, task.connections - 1)

            if last_error:
                raise last_error

        workers = [asyncio.create_task(_worker(c)) for c in task.chunks]
        try:
            await asyncio.gather(*workers)
        except Exception:
            for w in workers:
                if not w.done():
                    w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

    async def _progress_tracker(self, task: DownloadTask):
        """定期计算速度和 ETA，触发进度回调"""
        try:
            while task.status in (TaskStatus.DOWNLOADING, TaskStatus.PAUSED):
                await asyncio.sleep(0.5)

                if task.status == TaskStatus.PAUSED:
                    continue

                now = time.monotonic()
                elapsed = now - task._last_time
                if elapsed > 0:
                    bytes_delta = task.downloaded - task._last_bytes
                    instant_speed = bytes_delta / elapsed

                    # 滑动窗口平均（3 秒）
                    task._speed_samples.append(instant_speed)
                    if len(task._speed_samples) > 3:
                        task._speed_samples.pop(0)
                    task.speed = sum(task._speed_samples) / len(task._speed_samples)

                    # ETA
                    remaining = task.total_size - task.downloaded
                    task.eta = remaining / task.speed if task.speed > 0 else 0

                    task._last_time = now
                    task._last_bytes = task.downloaded

                # 触发进度回调
                if task._on_progress:
                    try:
                        await task._on_progress(task)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass

    def _cleanup_file(self, task: DownloadTask):
        """清理临时文件，释放 fallocate 预分配的空间"""
        try:
            if os.path.exists(task.temp_path):
                os.remove(task.temp_path)
                logger.info(f"[下载器] 清理临时文件: {task.temp_path}")
        except Exception as e:
            logger.warning(f"[下载器] 清理失败: {task.temp_path}, {e}")
        try:
            if os.path.exists(task.dest_path) and task.status != TaskStatus.COMPLETED:
                os.remove(task.dest_path)
        except Exception:
            pass
        self._cleanup_empty_parent(task.dest_path)

    def _cleanup_empty_parent(self, filepath: str):
        try:
            root = DOWNLOAD_DIR.resolve()
            current = Path(filepath).resolve().parent
            while current != root and root in current.parents:
                current.rmdir()
                current = current.parent
        except Exception:
            pass


def _fallocate(filepath: str, size: int):
    """预分配磁盘空间（Linux 用 fallocate，其他平台用 seek+truncate）"""
    try:
        # 确保父目录存在
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

        if platform.system() == "Linux" and hasattr(os, "posix_fallocate"):
            fd = os.open(filepath, os.O_CREAT | os.O_WRONLY)
            try:
                os.posix_fallocate(fd, 0, size)
            finally:
                os.close(fd)
            logger.debug(f"[fallocate] 预分配 {_format_size(size)}: {filepath}")
        else:
            # Windows / macOS: 用 truncate 快速创建指定大小的文件
            with open(filepath, "wb") as f:
                f.truncate(size)
            logger.debug(f"[truncate] 预分配 {_format_size(size)}: {filepath}")
    except Exception as e:
        logger.warning(f"[fallocate] 预分配失败（不影响下载）: {e}")
        # 即使预分配失败也不影响下载，文件会动态增长


def _format_size(size: int) -> str:
    if size <= 0:
        return "0 B"
    if size < 1024:
        return f"{size} B"
    elif size < 1024 ** 2:
        return f"{size / 1024:.1f} KB"
    elif size < 1024 ** 3:
        return f"{size / 1024 ** 2:.1f} MB"
    else:
        return f"{size / 1024 ** 3:.2f} GB"


def _format_speed(speed: float) -> str:
    if speed <= 0:
        return "0 B/s"
    if speed < 1024:
        return f"{speed:.0f} B/s"
    elif speed < 1024 ** 2:
        return f"{speed / 1024:.1f} KB/s"
    elif speed < 1024 ** 3:
        return f"{speed / 1024 ** 2:.1f} MB/s"
    else:
        return f"{speed / 1024 ** 3:.1f} GB/s"


def _format_eta(seconds: float) -> str:
    if seconds <= 0:
        return ""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    elif s < 3600:
        return f"{s // 60}m {s % 60}s"
    else:
        h = s // 3600
        m = (s % 3600) // 60
        return f"{h}h {m}m"


# ── 模块级单例 ──
downloader = BuiltinDownloader()
