"""TelDrive API 客户端 - 通过 REST API 与 TelDrive 通信

参考 OpenList TelDrive 驱动实现：
https://github.com/OpenListTeam/OpenList/tree/main/drivers/teldrive
"""

import asyncio
import aiohttp
import uuid
import hashlib
import math
import logging
from pathlib import Path
from typing import Optional, Callable, List, Dict, Any

logger = logging.getLogger(__name__)

# TelDrive 上传分块大小映射
CHUNK_SIZE_MAP = {
    "100M": 100 * 1024 * 1024,
    "200M": 200 * 1024 * 1024,
    "500M": 500 * 1024 * 1024,
    "1G": 1024 * 1024 * 1024,
    "2G": 2 * 1024 * 1024 * 1024,
}


class TelDriveClient:
    """TelDrive REST API 客户端"""

    # 默认超时（普通 API 请求）
    DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30, sock_read=60)
    # 上传超时的 connect 和 sock_read 基准值（total 由 _chunk_timeout 动态计算）
    UPLOAD_CONNECT_TIMEOUT = 30
    UPLOAD_SOCK_READ_TIMEOUT = 120

    def __init__(self, api_host: str = "http://localhost:8080",
                 access_token: str = "", channel_id: int = 0,
                 chunk_size: str = "500M", upload_concurrency: int = 4,
                 random_chunk_name: bool = True, max_retries: int = 3):
        self.api_host = api_host.rstrip("/")
        self.access_token = access_token
        self.channel_id = channel_id
        self.chunk_size_str = chunk_size
        self.chunk_size = CHUNK_SIZE_MAP.get(chunk_size, 500 * 1024 * 1024)
        self.upload_concurrency = upload_concurrency
        self.random_chunk_name = random_chunk_name
        self.max_retries = max_retries

    def _get_headers(self) -> dict:
        """获取请求头"""
        return {
            "Cookie": f"access_token={self.access_token}"
        }

    # ===========================================
    # 连接与基础 API
    # ===========================================

    async def test_connection(self) -> dict:
        """测试 TelDrive 连接"""
        try:
            async with aiohttp.ClientSession(timeout=self.DEFAULT_TIMEOUT) as session:
                async with session.get(
                    f"{self.api_host}/api/auth/session",
                    headers=self._get_headers()
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        username = data.get("userName", data.get("name", "unknown"))
                        return {
                            "success": True,
                            "message": f"TelDrive 连接成功，用户: {username}",
                            "version": None
                        }
                    else:
                        text = await resp.text()
                        return {
                            "success": False,
                            "message": f"TelDrive 认证失败 (HTTP {resp.status}): {text}",
                            "version": None
                        }
        except Exception as e:
            return {
                "success": False,
                "message": f"TelDrive 连接失败: {str(e)}",
                "version": None
            }

    async def create_directory(self, path: str) -> dict:
        """创建目录 - POST /api/files/mkdir"""
        async with aiohttp.ClientSession(timeout=self.DEFAULT_TIMEOUT) as session:
            async with session.post(
                f"{self.api_host}/api/files/mkdir",
                headers={**self._get_headers(), "Content-Type": "application/json"},
                json={"path": path}
            ) as resp:
                if resp.status == 204:
                    return {"success": True}
                try:
                    return await resp.json()
                except Exception:
                    return {"success": resp.status < 300}

    async def list_files(self, path: str = "/") -> list:
        """列出目录文件"""
        async with aiohttp.ClientSession(timeout=self.DEFAULT_TIMEOUT) as session:
            async with session.get(
                f"{self.api_host}/api/files",
                headers=self._get_headers(),
                params={"path": path}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("items", data if isinstance(data, list) else [])
                return []

    # ===========================================
    # 文件查找与删除（参考 util.go 的 getFile / driver.go 的 Remove）
    # ===========================================

    async def _find_file(self, session: aiohttp.ClientSession,
                         path: str, name: str, is_folder: bool = False) -> Optional[Dict]:
        """查找文件 — 对标 util.go 的 getFile"""
        params = {
            "path": path,
            "name": name,
            "type": "folder" if is_folder else "file",
            "operation": "find",
        }
        async with session.get(
            f"{self.api_host}/api/files",
            headers=self._get_headers(),
            params=params
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                items = data.get("items", [])
                if items:
                    return items[0]
        return None

    async def _delete_file(self, session: aiohttp.ClientSession, file_id: str) -> bool:
        """删除文件 — 对标 driver.go 的 Remove"""
        async with session.post(
            f"{self.api_host}/api/files/delete",
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json={"ids": [file_id]}
        ) as resp:
            return resp.status < 300

    # ===========================================
    # 上传辅助方法（参考 upload.go）
    # ===========================================

    async def _get_file_parts(self, session: aiohttp.ClientSession,
                              upload_id: str) -> List[Dict]:
        """获取已上传的 part 列表 — 对标 upload.go 的 getFilePart"""
        async with session.get(
            f"{self.api_host}/api/uploads/{upload_id}",
            headers=self._get_headers()
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    return data
            return []

    async def _check_part_exists(self, session: aiohttp.ClientSession,
                                  upload_id: str, part_no: int) -> Optional[Dict]:
        """检查某个 part 是否已存在 — 对标 upload.go 的 checkFilePartExist"""
        parts = await self._get_file_parts(session, upload_id)
        for part in parts:
            if part.get("partId") == part_no or part.get("partNo") == part_no:
                return part
        return None

    async def _touch(self, session: aiohttp.ClientSession,
                     name: str, path: str) -> dict:
        """创建空文件 — 对标 upload.go 的 touch"""
        async with session.post(
            f"{self.api_host}/api/files",
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json={"name": name, "type": "file", "path": path}
        ) as resp:
            if resp.status in (200, 201):
                return {"success": True, "data": await resp.json()}
            text = await resp.text()
            return {"success": False, "error": f"创建空文件失败 (HTTP {resp.status}): {text}"}

    async def _cleanup_upload(self, session: aiohttp.ClientSession,
                               upload_id: str) -> None:
        """清理上传记录 — 对标 driver.go Put 方法的 defer delete"""
        try:
            async with session.delete(
                f"{self.api_host}/api/uploads/{upload_id}",
                headers=self._get_headers()
            ) as resp:
                logger.debug(f"清理上传记录 {upload_id}: HTTP {resp.status}")
        except Exception as e:
            logger.debug(f"清理上传记录失败: {e}")

    @staticmethod
    def _md5_hash(text: str) -> str:
        """生成 MD5 哈希 — 对标 upload.go 的 getMD5Hash"""
        return hashlib.md5(text.encode()).hexdigest()

    def _get_part_name(self, filename: str, part_no: int, total_parts: int) -> str:
        """生成 part 名称 — 对标 upload.go 中的命名逻辑"""
        if self.random_chunk_name:
            return self._md5_hash(str(uuid.uuid4()))
        if total_parts <= 1:
            return filename
        return f"{filename}.{part_no}"

    # ===========================================
    # 单块上传请求（含重试）— 对标 upload.go 的 uploadSingleChunk
    # ===========================================

    @staticmethod
    def _chunk_timeout(chunk_size: int) -> aiohttp.ClientTimeout:
        """根据 chunk 大小动态计算上传超时（保底 5 分钟，每 100MB 加 3 分钟）"""
        base = 300  # 5 min 保底
        extra = int(chunk_size / (100 * 1024 * 1024)) * 180  # 每 100MB +3min
        total = base + extra
        return aiohttp.ClientTimeout(
            total=total,
            connect=TelDriveClient.UPLOAD_CONNECT_TIMEOUT,
            sock_read=TelDriveClient.UPLOAD_SOCK_READ_TIMEOUT,
        )

    async def _upload_single_chunk(self, session: aiohttp.ClientSession,
                                    upload_id: str,
                                    file_path: Path,
                                    chunk_offset: int,
                                    chunk_size: int,
                                    part_no: int, filename: str,
                                    total_parts: int,
                                    progress_callback: Optional[Callable] = None,
                                    file_size: int = 0) -> Dict:
        """上传单个 chunk，流式从文件读取+发送，不将整块一次性读入内存。

        相比旧版 bytes 模式：
        - 内存占用从 chunk_size(~500MB) 降低到 STREAM_BLOCK(1MB)
        - 使用 memoryview 避免切片时额外内存复制
        """
        retry_count = 0
        # 流式发送粒度：1MB — 同一时刻只有 1 个 buffer 在内存中
        STREAM_BLOCK = 1024 * 1024

        while True:
            # 断点续传：检查 part 是否已存在
            existing = await self._check_part_exists(session, upload_id, part_no)
            if existing and existing.get("name"):
                logger.info(f"  块 {part_no}/{total_parts} 已存在，跳过上传")
                if progress_callback and file_size > 0:
                    await progress_callback(chunk_offset + chunk_size, file_size)
                return existing

            part_name = self._get_part_name(filename, part_no, total_parts)

            headers = self._get_headers()
            headers["Content-Type"] = "application/octet-stream"
            headers["Content-Length"] = str(chunk_size)

            params = {
                "partName": part_name,
                "partNo": str(part_no),
                "fileName": filename,
            }

            try:
                # 流式读文件+发送：每次只读 STREAM_BLOCK 到内存
                # 用 memoryview 避免切片时额外的 bytes 拷贝
                _file_path = file_path  # 闭包捕获
                _chunk_offset = chunk_offset
                _chunk_size = chunk_size

                async def data_sender():
                    sent = 0
                    loop = asyncio.get_event_loop()
                    # 在线程池中打开文件并逐块读取，避免阻塞事件循环
                    buf = bytearray(STREAM_BLOCK)
                    mv = memoryview(buf)

                    def _read_block(offset: int, size: int) -> bytes:
                        with open(_file_path, "rb") as f:
                            f.seek(offset)
                            n = f.readinto(buf[:size] if size < STREAM_BLOCK else buf)
                            return bytes(mv[:n]) if n else b""

                    while sent < _chunk_size:
                        to_read = min(STREAM_BLOCK, _chunk_size - sent)
                        block = await loop.run_in_executor(
                            None, _read_block, _chunk_offset + sent, to_read
                        )
                        if not block:
                            break
                        yield block
                        sent += len(block)
                        if progress_callback and file_size > 0:
                            await progress_callback(_chunk_offset + sent, file_size)

                # 使用动态超时，大 chunk 给更多时间
                timeout = self._chunk_timeout(_chunk_size)
                async with session.post(
                    f"{self.api_host}/api/uploads/{upload_id}",
                    headers=headers,
                    data=data_sender(),
                    params=params,
                    timeout=timeout,
                ) as resp:
                    if resp.status in (200, 201):
                        result = await resp.json()
                        if result.get("name") or result.get("partId") is not None:
                            return result
                        raise Exception(f"上传块 {part_no} 响应缺少有效数据: {result}")
                    else:
                        text = await resp.text()
                        raise Exception(f"HTTP {resp.status}: {text}")
            except asyncio.CancelledError:
                raise  # 被取消时立即退出，不重试
            except Exception as e:
                retry_count += 1
                if retry_count > self.max_retries:
                    raise Exception(f"上传块 {part_no} 在 {self.max_retries} 次重试后仍然失败: {e}")

                backoff = min(retry_count * retry_count, 30)
                logger.warning(f"  块 {part_no} 上传失败: {e}，{backoff}s 后第 {retry_count} 次重试")
                await asyncio.sleep(backoff)

    # ===========================================
    # 创建文件记录（含校验）— 对标 upload.go 的 createFileOnUploadSuccess
    # ===========================================

    async def _create_file_record(self, session: aiohttp.ClientSession,
                                   name: str, upload_id: str, path: str,
                                   uploaded_parts: List[Dict],
                                   total_size: int) -> dict:
        """上传完成后校验 parts 并创建文件记录"""
        # 校验：比对远程 parts 数量与本地上传的数量
        remote_parts = await self._get_file_parts(session, upload_id)
        if len(remote_parts) != len(uploaded_parts):
            logger.warning(
                f"Parts 数量不一致: 本地 {len(uploaded_parts)}, 远程 {len(remote_parts)}"
            )

        # 构建 parts 列表（使用远程返回的 partId 和 salt）
        format_parts = []
        for p in remote_parts:
            part_entry = {"id": p.get("partId", p.get("id"))}
            if p.get("salt"):
                part_entry["salt"] = p["salt"]
            format_parts.append(part_entry)

        file_data = {
            "name": name,
            "type": "file",
            "path": path,
            "parts": format_parts,
            "size": total_size,
        }

        async with session.post(
            f"{self.api_host}/api/files",
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json=file_data
        ) as resp:
            if resp.status in (200, 201):
                result = await resp.json()
                return {"success": True, "data": result}
            else:
                text = await resp.text()
                return {
                    "success": False,
                    "error": f"创建文件记录失败 (HTTP {resp.status}): {text}"
                }

    # ===========================================
    # 串行上传 — 对标 upload.go 的 doSingleUpload
    # ===========================================

    async def _do_single_upload(self, session: aiohttp.ClientSession,
                                 file_path: Path, upload_id: str,
                                 filename: str, file_size: int,
                                 total_parts: int,
                                 progress_callback: Optional[Callable]) -> List[Dict]:
        """串行逐块上传 — 流式读取，每次只有 1MB 在内存中"""
        parts = []
        offset = 0
        part_no = 1

        while offset < file_size:
            cur_chunk_size = min(self.chunk_size, file_size - offset)
            logger.info(f"  上传块 {part_no}/{total_parts} ({cur_chunk_size} bytes)")

            part_result = await self._upload_single_chunk(
                session, upload_id, file_path,
                chunk_offset=offset,
                chunk_size=cur_chunk_size,
                part_no=part_no, filename=filename, total_parts=total_parts,
                progress_callback=progress_callback,
                file_size=file_size
            )
            parts.append(part_result)

            offset += cur_chunk_size
            part_no += 1

        return parts

    # ===========================================
    # 并发上传 — 对标 upload.go 的 doMultiUpload
    # ===========================================

    async def _do_multi_upload(self, session: aiohttp.ClientSession,
                                file_path: Path, upload_id: str,
                                filename: str, file_size: int,
                                total_parts: int,
                                progress_callback: Optional[Callable]) -> List[Dict]:
        """并发分块上传 — 流式读取文件，不再将 chunk 读入内存。

        内存占用: upload_concurrency × STREAM_BLOCK(1MB) ≈ 4MB（而非旧版 ~2GB）
        """
        sem = asyncio.Semaphore(self.upload_concurrency)
        results: Dict[int, Dict] = {}
        lock = asyncio.Lock()

        # 构建 chunk 描述表（仅偏移量+大小，不读文件）
        chunks_info = []
        offset = 0
        part_no = 1
        while offset < file_size:
            cur_chunk_size = min(self.chunk_size, file_size - offset)
            chunks_info.append((part_no, offset, cur_chunk_size))
            offset += cur_chunk_size
            part_no += 1

        async def upload_chunk(p_no: int, p_offset: int, p_size: int):
            async with sem:
                # 并发上传时，进度通过 lock 累加已发送字节
                async def concurrent_progress(sent_total: int, _total: int):
                    chunk_sent = sent_total - p_offset
                    async with lock:
                        current_total = sum(
                            ci[2] for ci in chunks_info
                            if ci[0] in results  # 已完成的块
                        ) + chunk_sent
                        if progress_callback:
                            await progress_callback(current_total, file_size)

                logger.info(f"  并发上传块 {p_no}/{total_parts} ({p_size} bytes)")
                part_result = await self._upload_single_chunk(
                    session, upload_id, file_path,
                    chunk_offset=p_offset,
                    chunk_size=p_size,
                    part_no=p_no, filename=filename, total_parts=total_parts,
                    progress_callback=concurrent_progress,
                    file_size=file_size
                )
                results[p_no] = part_result

        # 创建所有上传任务
        tasks = [
            asyncio.create_task(upload_chunk(p_no, p_offset, p_size))
            for p_no, p_offset, p_size in chunks_info
        ]

        # 等待所有任务完成（任一失败则抛出异常）
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        # 检查是否有异常
        for task in done:
            if task.exception():
                # 取消剩余任务
                for p in pending:
                    p.cancel()
                raise task.exception()

        # 按 part_no 排序返回
        sorted_parts = [results[k] for k in sorted(results.keys())]
        return sorted_parts

    # ===========================================
    # 主上传入口 — 对标 driver.go 的 Put 方法
    # ===========================================

    async def upload_file_chunked(self, file_path: str, teldrive_path: str = "/",
                                   progress_callback: Callable = None) -> dict:
        """上传文件到 TelDrive（完整流程）

        流程（参考 OpenList driver.go 的 Put 方法）：
        1. 生成 upload_id (UUID)
        2. 查找并删除同名文件
        3. 初始化上传会话
        4. 空文件 → touch
        5. 单块文件 → 串行上传
        6. 多块文件 → 并发上传
        7. 创建文件记录（含 parts 校验）
        8. finally: 清理上传记录

        Args:
            file_path: 本地文件路径
            teldrive_path: TelDrive 目标路径
            progress_callback: 进度回调函数 (uploaded_bytes, total_bytes)

        Returns:
            上传结果 dict
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_size = file_path.stat().st_size
        filename = file_path.name
        upload_id = str(uuid.uuid4())

        total_parts = int(math.ceil(file_size / self.chunk_size)) if file_size > 0 else 0

        logger.info(f"开始上传: {filename} ({file_size} bytes, {total_parts} 块, "
                     f"并发={self.upload_concurrency}, chunk={self.chunk_size_str})")

        # 确保目标目录存在
        if teldrive_path != "/":
            try:
                await self.create_directory(teldrive_path)
            except Exception:
                pass

        # session 不设全局 total 超时，由每个 chunk 的 _chunk_timeout 动态控制
        upload_session_timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.UPLOAD_CONNECT_TIMEOUT,
            sock_read=self.UPLOAD_SOCK_READ_TIMEOUT,
        )
        async with aiohttp.ClientSession(timeout=upload_session_timeout) as session:
            try:
                # 步骤 1: 查找并删除同名文件（对标 driver.go Put 中的逻辑）
                existing_file = await self._find_file(session, teldrive_path, filename)
                if existing_file:
                    file_id = existing_file.get("id")
                    if file_id:
                        logger.info(f"发现同名文件 {filename} (id={file_id})，删除后重新上传")
                        await self._delete_file(session, file_id)

                # 步骤 2: 初始化上传会话 — GET /api/uploads/{uploadId}
                async with session.get(
                    f"{self.api_host}/api/uploads/{upload_id}",
                    headers=self._get_headers()
                ) as resp:
                    logger.debug(f"初始化上传会话: HTTP {resp.status}")

                # 步骤 3: 空文件处理
                if file_size == 0:
                    logger.info(f"空文件，直接创建记录")
                    return await self._touch(session, filename, teldrive_path)

                # 步骤 4: 上传分块
                if total_parts <= 1:
                    uploaded_parts = await self._do_single_upload(
                        session, file_path, upload_id, filename,
                        file_size, total_parts, progress_callback
                    )
                else:
                    uploaded_parts = await self._do_multi_upload(
                        session, file_path, upload_id, filename,
                        file_size, total_parts, progress_callback
                    )

                # 步骤 5: 创建文件记录（含 parts 校验）
                result = await self._create_file_record(
                    session, filename, upload_id, teldrive_path,
                    uploaded_parts, file_size
                )

                if result.get("success"):
                    logger.info(f"文件 {filename} 上传成功")
                else:
                    logger.error(f"文件 {filename} 创建记录失败: {result.get('error')}")

                return result

            except Exception as e:
                logger.error(f"上传文件失败: {e}")
                return {"success": False, "error": str(e)}

            finally:
                # 步骤 6: 清理上传记录（对标 driver.go Put 的 defer）
                await self._cleanup_upload(session, upload_id)
