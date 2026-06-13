"""TelDrive API 客户端 - 通过 REST API 与 TelDrive 通信

参考 OpenList TelDrive 驱动实现：
https://github.com/OpenListTeam/OpenList/tree/main/drivers/teldrive
"""

import asyncio
import aiohttp
import uuid
import hashlib
import math
import re
import logging
import json
from pathlib import Path
from typing import Optional, Callable, List, Dict, Any

logger = logging.getLogger(__name__)

# TelDrive 上传分块大小映射（已知预设的快速路径）
CHUNK_SIZE_MAP = {
    "100M": 100 * 1024 * 1024,
    "200M": 200 * 1024 * 1024,
    "250M": 250 * 1024 * 1024,
    "500M": 500 * 1024 * 1024,
    "1G": 1024 * 1024 * 1024,
    "2G": 2 * 1024 * 1024 * 1024,
}

_CHUNK_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?)B?\s*$", re.IGNORECASE)
_CHUNK_UNIT_MULT = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
DEFAULT_CHUNK_SIZE = 500 * 1024 * 1024


def parse_chunk_size(value, default: int = DEFAULT_CHUNK_SIZE) -> int:
    """将分块大小（如 "250M" / "1G" / 整数字节）解析为字节数。

    优先命中已知预设；否则按 "<数字><单位>" 通用解析（K/M/G/T，可省略单位=字节）。
    无法解析时回退 default，并记录告警——避免像旧版那样把未登记的 "250M" 静默当成 500M。
    """
    if isinstance(value, (int, float)):
        return int(value) if value > 0 else default
    key = str(value or "").strip()
    if key in CHUNK_SIZE_MAP:
        return CHUNK_SIZE_MAP[key]
    match = _CHUNK_SIZE_RE.match(key)
    if not match:
        logger.warning(f"无法解析上传分块大小 {value!r}，回退默认 {default} 字节")
        return default
    size = int(float(match.group(1)) * _CHUNK_UNIT_MULT[match.group(2).upper()])
    return size if size > 0 else default


class ChunkSourceReadError(IOError):
    """本地源文件读取异常（被截断/删除）——重试同一块没有意义"""


class ChunkUploadHTTPError(Exception):
    """chunk 上传收到明确的 HTTP 错误响应"""

    def __init__(self, status: int, text: str):
        self.status = status
        super().__init__(f"HTTP {status}: {text}")


class TelDriveClient:
    """TelDrive REST API 客户端"""

    # 默认超时（普通 API 请求）
    DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30, sock_read=60)
    # 上传超时的 connect 和 sock_read 基准值（total 由 _chunk_timeout 动态计算）。
    # sock_read 必须容忍“请求体已发完、TelDrive 正在向 Telegram 推送”的静默期，
    # 过小会造成客户端超时但服务端最终落块 → 同号重复 part。
    UPLOAD_CONNECT_TIMEOUT = 30
    UPLOAD_SOCK_READ_TIMEOUT = 300
    # 结果不明确失败后的最小落块轮询窗口（秒）；测试可调小
    AMBIGUOUS_POLL_FLOOR_SECONDS = 60

    def __init__(self, api_host: str = "http://localhost:8080",
                 access_token: str = "", channel_id: int = 0,
                 chunk_size: str = "500M", upload_concurrency: int = 4,
                 random_chunk_name: bool = True, max_retries: int = 3,
                 min_throughput_kbps: int = 100):
        self.api_host = api_host.rstrip("/")
        self.access_token = access_token
        self.channel_id = channel_id
        self.chunk_size_str = chunk_size
        self.chunk_size = parse_chunk_size(chunk_size)
        self.upload_concurrency = upload_concurrency
        self.random_chunk_name = random_chunk_name
        self.max_retries = max_retries
        # 超时计算假设的最低可接受吞吐（KB/s）；低于该速度判定为链路异常
        self.min_throughput_kbps = max(16, int(min_throughput_kbps or 100))

    def _get_headers(self) -> dict:
        """获取请求头"""
        return {
            "Cookie": f"access_token={self.access_token}"
        }

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        try:
            if value in (None, ""):
                return None
            return int(str(value).strip())
        except Exception:
            return None

    def _get_part_number(self, part: Dict[str, Any]) -> Optional[int]:
        return self._coerce_int(part.get("partNo"))

    def _get_part_message_id(self, part: Dict[str, Any]) -> Optional[int]:
        return self._coerce_int(part.get("partId", part.get("id")))

    def _extract_confirmed_part_numbers(self, parts: List[Dict[str, Any]]) -> List[int]:
        numbers = []
        seen = set()
        for part in parts or []:
            number = self._get_part_number(part)
            if number is None or number in seen:
                continue
            seen.add(number)
            numbers.append(number)
        return sorted(numbers)

    def _normalize_confirmed_part_numbers(self, part_numbers: Any, total_parts: int = 0) -> List[int]:
        normalized = []
        seen = set()
        for value in list(part_numbers or []):
            number = self._coerce_int(value)
            if number is None or number <= 0:
                continue
            if total_parts > 0 and number > total_parts:
                continue
            if number in seen:
                continue
            seen.add(number)
            normalized.append(number)
        return sorted(normalized)

    def _normalize_remote_parts(self, remote_parts: Any, total_parts: int = 0) -> List[Dict[str, Any]]:
        normalized: list[Dict[str, Any]] = []
        for part in list(remote_parts or []):
            if not isinstance(part, dict):
                continue
            number = self._get_part_number(part)
            message_id = self._get_part_message_id(part)
            if number is None or message_id is None or number <= 0:
                continue
            if total_parts > 0 and number > total_parts:
                normalized.append(dict(part))
                continue
            item = dict(part)
            item["partNo"] = number
            item["partId"] = message_id
            normalized.append(item)
        return normalized

    def _build_remote_parts_map(self, remote_parts: Any, total_parts: int = 0) -> Dict[int, Dict[str, Any]]:
        result: Dict[int, Dict[str, Any]] = {}
        for part in self._normalize_remote_parts(remote_parts, total_parts):
            number = self._get_part_number(part)
            if number is None or number in result:
                continue
            result[number] = dict(part)
        return result

    @staticmethod
    def _structured_error(error_code: str, message: str, **extra) -> dict:
        payload = {"code": error_code, "message": message}
        if extra:
            payload["details"] = extra
        return {
            "success": False,
            "error": f"structured_upload_error::{json.dumps(payload, ensure_ascii=False)}",
            "error_code": error_code,
            "error_details": extra,
        }

    def _validate_remote_parts(self, remote_parts: List[Dict[str, Any]], total_parts: int) -> Optional[dict]:
        if len(remote_parts) != total_parts:
            return self._structured_error(
                "remote_parts_count_mismatch",
                f"远端分块数量异常: expected={total_parts}, actual={len(remote_parts)}",
                expected_total_parts=total_parts,
                actual_total_parts=len(remote_parts),
            )

        message_ids = [self._get_part_message_id(part) for part in remote_parts]
        if any(message_id is None for message_id in message_ids):
            return self._structured_error(
                "remote_parts_invalid_shape",
                "远端分块缺少有效的 partId/id",
                expected_total_parts=total_parts,
                actual_total_parts=len(remote_parts),
            )

        part_numbers = [self._get_part_number(part) for part in remote_parts]
        if any(number is None for number in part_numbers):
            return self._structured_error(
                "remote_parts_invalid_shape",
                "远端分块缺少有效的 partNo",
                expected_total_parts=total_parts,
                actual_total_parts=len(remote_parts),
            )

        numbers = [int(number) for number in part_numbers if number is not None]
        expected_numbers = list(range(1, total_parts + 1))
        if sorted(numbers) != expected_numbers:
            error_code = "remote_parts_duplicate_numbers" if len(set(numbers)) != len(numbers) else "remote_parts_invalid_shape"
            return self._structured_error(
                error_code,
                f"远端分块编号异常: expected=1..{total_parts}, actual={numbers}",
                expected_total_parts=total_parts,
                actual_total_parts=len(remote_parts),
                remote_part_numbers=numbers,
            )

        return None

    def _order_remote_parts(self, remote_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted((dict(part) for part in remote_parts), key=lambda part: self._get_part_number(part) or 0)

    def _get_part_size(self, part: Dict[str, Any]) -> Optional[int]:
        return self._coerce_int(part.get("size"))

    def _expected_part_size(self, file_size: int, part_no: int, total_parts: int) -> int:
        """按当前 chunk_size 推算某个 partNo 应有的字节数（最后一块为余数）"""
        if file_size <= 0 or part_no <= 0 or (total_parts > 0 and part_no > total_parts):
            return 0
        start = (part_no - 1) * self.chunk_size
        if start >= file_size:
            return 0
        return min(self.chunk_size, file_size - start)

    def _part_size_matches(self, part: Dict[str, Any], expected_size: int) -> bool:
        """远端 part 尺寸校验；远端未提供 size 或预期未知时视为通过"""
        if expected_size <= 0:
            return True
        actual = self._get_part_size(part)
        return actual is None or actual == expected_size

    def _dedupe_remote_parts(self, remote_parts: Any, total_parts: int,
                             file_size: int) -> tuple[Dict[int, Dict[str, Any]], List[Dict[str, Any]]]:
        """同号重复 part 自愈：每个 partNo 择优保留一个，其余作为孤儿返回。

        客户端超时重传可能导致服务端存有两份同号 part（内容相同），
        旧行为是整体判污染并清空会话重传；这里改为数据级去重：
        - 优先选尺寸与预期一致的 part（防 chunk_size 变更后的错位块）
        - 同尺寸时选 partId 最大的（最近一次上传）
        - 落选的 part 收集为孤儿，由上层负责删除其 Telegram 消息
        """
        by_number: Dict[int, List[Dict[str, Any]]] = {}
        orphans: List[Dict[str, Any]] = []
        for part in list(remote_parts or []):
            if not isinstance(part, dict):
                continue
            number = self._get_part_number(part)
            message_id = self._get_part_message_id(part)
            if number is None or message_id is None or number <= 0 or (
                total_parts > 0 and number > total_parts
            ):
                if message_id is not None:
                    orphans.append(dict(part))
                continue
            item = dict(part)
            item["partNo"] = number
            item["partId"] = message_id
            by_number.setdefault(number, []).append(item)

        selected: Dict[int, Dict[str, Any]] = {}
        for number, candidates in by_number.items():
            expected_size = self._expected_part_size(file_size, number, total_parts)
            matching = [c for c in candidates if self._part_size_matches(c, expected_size)]
            pool = matching or candidates
            best = max(pool, key=lambda c: self._get_part_message_id(c) or 0)
            selected[number] = best
            orphans.extend(c for c in candidates if c is not best)
        return selected, orphans

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
        """删除文件 — 对标 driver.go 的 Remove

        删除前先通知 tel2teldrive 这是内部删除，避免删除同步把
        "同名覆盖" 误判为外部消失并触发误删/映射错乱。
        """
        self._notify_internal_file_deletion(file_id)
        async with session.post(
            f"{self.api_host}/api/files/delete",
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json={"ids": [file_id]}
        ) as resp:
            return resp.status < 300

    @staticmethod
    def _notify_internal_file_deletion(file_id: str) -> None:
        try:
            from app.modules.tel2teldrive.service import remember_internal_deleted_file_ids
            remember_internal_deleted_file_ids([str(file_id)])
        except Exception as e:
            logger.debug(f"notify internal file deletion failed: {e}")

    # ===========================================
    # 上传辅助方法（参考 upload.go）
    # ===========================================

    async def _get_file_parts(self, session: aiohttp.ClientSession,
                              upload_id: str) -> List[Dict]:
        """获取已上传的 part 列表 — 对标 upload.go 的 getFilePart

        非 200 响应抛出异常：绝不把"查询失败"当成"没有 part"，
        否则断点续传会误判 part 不存在而重复上传同号分块（产生污染）。
        """
        async with session.get(
            f"{self.api_host}/api/uploads/{upload_id}",
            headers=self._get_headers(),
            timeout=self.DEFAULT_TIMEOUT,
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    return data
                return []
            if resp.status == 404:
                # 新建/已清理的上传会话：确定性的"无分块"
                return []
            text = await resp.text()
            raise ChunkUploadHTTPError(resp.status, f"查询上传分块列表失败: {text}")

    async def _get_file_parts_with_retry(self, session: aiohttp.ClientSession,
                                         upload_id: str, attempts: int = 3) -> List[Dict]:
        """带退避重试的 parts 查询，用于上传前/校验前的关键路径"""
        last_error: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                return await self._get_file_parts(session, upload_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = e
                if attempt < attempts - 1:
                    await asyncio.sleep(min(2 ** attempt, 8))
        raise Exception(f"查询上传分块列表在 {attempts} 次尝试后仍失败: {last_error}")

    async def _check_part_exists(self, session: aiohttp.ClientSession,
                                  upload_id: str, part_no: int,
                                  remote_parts_map: Optional[Dict[int, Dict[str, Any]]] = None,
                                  expected_size: int = 0) -> Optional[Dict]:
        """检查某个 part 是否已存在 — 对标 upload.go 的 checkFilePartExist

        expected_size > 0 时校验远端 part 尺寸，不符的 part 视为不可复用
        （chunk_size 配置变更或污染块），避免拼出损坏文件。
        """
        if remote_parts_map is not None and part_no in remote_parts_map:
            cached = remote_parts_map[part_no]
            if self._part_size_matches(cached, expected_size):
                return dict(cached)
            remote_parts_map.pop(part_no, None)
        parts = await self._get_file_parts_with_retry(session, upload_id)
        candidates = [
            part for part in parts
            if self._get_part_number(part) == part_no
            and self._get_part_message_id(part) is not None
        ]
        matching = [c for c in candidates if self._part_size_matches(c, expected_size)]
        if not matching:
            if candidates:
                logger.warning(
                    f"  块 {part_no} 远端存在 {len(candidates)} 份但尺寸均不符 "
                    f"(expected={expected_size})，视为不可复用"
                )
            return None
        best = max(matching, key=lambda c: self._get_part_message_id(c) or 0)
        if remote_parts_map is not None:
            remote_parts_map[part_no] = dict(best)
        return dict(best)

    async def get_upload_parts(self, upload_id: str) -> List[Dict]:
        async with aiohttp.ClientSession(timeout=self.DEFAULT_TIMEOUT) as session:
            return await self._get_file_parts(session, upload_id)

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

    async def cleanup_upload_session(self, upload_id: str) -> None:
        async with aiohttp.ClientSession(timeout=self.DEFAULT_TIMEOUT) as session:
            await self._cleanup_upload(session, upload_id)

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

    def _chunk_timeout(self, chunk_size: int) -> aiohttp.ClientTimeout:
        """根据 chunk 大小与最低吞吐假设计算上传超时。

        total = 发送耗时(按 min_throughput_kbps) + 服务端推送 Telegram 的余量；
        保底 10 分钟。默认 100KB/s 下 500MB 块约 85 分钟 + 10 分钟余量。
        """
        transfer_seconds = int(chunk_size / (self.min_throughput_kbps * 1024))
        total = max(600, transfer_seconds + 600)
        return aiohttp.ClientTimeout(
            total=total,
            connect=self.UPLOAD_CONNECT_TIMEOUT,
            sock_read=self.UPLOAD_SOCK_READ_TIMEOUT,
        )

    def _ambiguous_failure_poll_window(self, chunk_size: int) -> int:
        """网络层失败后等待服务端异步落块的轮询窗口（秒）。

        客户端超时/断连时服务端可能仍在把数据推送到 Telegram 并最终落库；
        立即重传会产生同号重复 part。按 4MB/s 估算服务端推送耗时，60s 保底，300s 封顶。
        """
        estimated = int(chunk_size / (4 * 1024 * 1024))
        return max(self.AMBIGUOUS_POLL_FLOOR_SECONDS, min(300, estimated))

    @staticmethod
    def _find_source_read_error(exc: BaseException) -> Optional["ChunkSourceReadError"]:
        """沿异常因果链查找 ChunkSourceReadError。

        data_sender 内抛出的异常会被 aiohttp 包装成
        ClientOSError("Can not write request body")，原始异常在 __cause__/__context__ 中。
        """
        seen: set[int] = set()
        current: Optional[BaseException] = exc
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, ChunkSourceReadError):
                return current
            current = current.__cause__ or current.__context__
        return None

    async def _wait_part_landing(self, session: aiohttp.ClientSession,
                                 upload_id: str, part_no: int,
                                 expected_size: int,
                                 max_wait_seconds: int,
                                 remote_parts_map: Optional[Dict[int, Dict[str, Any]]] = None) -> Optional[Dict]:
        """失败后轮询确认 part 是否已被服务端落库，避免重复上传同号分块"""
        deadline = asyncio.get_event_loop().time() + max_wait_seconds
        interval = 5
        while True:
            try:
                existing = await self._check_part_exists(
                    session, upload_id, part_no, remote_parts_map, expected_size
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"  轮询块 {part_no} 落库状态失败: {e}")
                existing = None
            if existing and self._get_part_message_id(existing) is not None:
                return existing
            if asyncio.get_event_loop().time() >= deadline:
                return None
            await asyncio.sleep(interval)

    async def _upload_single_chunk(self, session: aiohttp.ClientSession,
                                    upload_id: str,
                                    file_path: Path,
                                    chunk_offset: int,
                                    chunk_size: int,
                                    part_no: int, filename: str,
                                    total_parts: int) -> Dict:
        """上传单个 chunk，流式从文件读取+发送，不将整块一次性读入内存。

        注意：这里不再按“本地已发送字节”上报进度；
        进度统一由上层在单个 part 被 TelDrive 确认成功后累计，
        这样更接近真实的远端上传完成度。

        相比旧版 bytes 模式：
        - 内存占用从 chunk_size(~500MB) 降低到 STREAM_BLOCK(1MB)
        - 使用 memoryview 避免切片时额外内存复制
        """

        retry_count = 0
        # 流式发送粒度：1MB — 同一时刻只有 1 个 buffer 在内存中
        STREAM_BLOCK = 1024 * 1024

        while True:
            # 断点续传：检查 part 是否已存在（含尺寸校验）。
            # 查询失败时走与上传失败相同的退避重试，绝不在"无法确认"时盲目上传
            try:
                existing = await self._check_part_exists(
                    session, upload_id, part_no, expected_size=chunk_size
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                retry_count += 1
                if retry_count > self.max_retries:
                    raise Exception(f"上传块 {part_no} 在 {self.max_retries} 次重试后仍然失败: {e}")
                backoff = min(retry_count * retry_count, 30)
                logger.warning(f"  块 {part_no} 状态查询失败: {e}，{backoff}s 后第 {retry_count} 次重试")
                await asyncio.sleep(backoff)
                continue
            if existing and self._get_part_message_id(existing) is not None:
                logger.info(f"  块 {part_no}/{total_parts} 已存在，跳过上传")
                result = dict(existing)
                result.setdefault("partNo", part_no)
                return result


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
                            # 必须用 memoryview 切片（与 buf 共享底层内存）；
                            # bytearray 切片是独立拷贝，readinto 会写入拷贝，
                            # 再从 mv 取数据就会返回上一块的旧内容（静默损坏）
                            n = f.readinto(mv[:size])
                            return bytes(mv[:n]) if n else b""

                    while sent < _chunk_size:
                        to_read = min(STREAM_BLOCK, _chunk_size - sent)
                        block = await loop.run_in_executor(
                            None, _read_block, _chunk_offset + sent, to_read
                        )
                        if not block:
                            # 文件被截断/删除时显式报错，绝不发送字节数不足的 part
                            raise ChunkSourceReadError(
                                f"读取本地文件提前 EOF: offset={_chunk_offset + sent}, "
                                f"chunk_size={_chunk_size}, sent={sent}, file={_file_path}"
                            )
                        yield block
                        sent += len(block)
                    if sent != _chunk_size:
                        raise ChunkSourceReadError(
                            f"块 {part_no} 实际发送 {sent} 字节，预期 {_chunk_size} 字节: {_file_path}"
                        )


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
                            result.setdefault("partNo", part_no)
                            return result
                        raise Exception(f"上传块 {part_no} 响应缺少有效数据: {result}")
                    else:
                        text = await resp.text()
                        raise ChunkUploadHTTPError(resp.status, text)
            except asyncio.CancelledError:
                raise  # 被取消时立即退出，不重试
            except ChunkSourceReadError:
                raise  # 本地文件异常，重试同一块没有意义
            except Exception as e:
                # data_sender 内的源文件错误会被 aiohttp 包装成
                # "Can not write request body"，从因果链还原后直接失败
                source_error = self._find_source_read_error(e)
                if source_error is not None:
                    raise source_error

                retry_count += 1
                if retry_count > self.max_retries:
                    raise Exception(f"上传块 {part_no} 在 {self.max_retries} 次重试后仍然失败: {e}")

                # 结果不明确的失败（超时/断连/5xx）：服务端可能仍在异步落块，
                # 立刻重传会产生同号重复 part（污染）。先轮询确认，再决定是否重传。
                is_definitive_reject = (
                    isinstance(e, ChunkUploadHTTPError) and 400 <= e.status < 500
                )
                if not is_definitive_reject:
                    poll_window = self._ambiguous_failure_poll_window(_chunk_size)
                    logger.warning(
                        f"  块 {part_no} 上传失败（结果不明确）: {e}，"
                        f"先轮询 {poll_window}s 确认服务端是否已落块"
                    )
                    landed = await self._wait_part_landing(
                        session, upload_id, part_no,
                        expected_size=_chunk_size,
                        max_wait_seconds=poll_window,
                    )
                    if landed:
                        logger.info(f"  块 {part_no}/{total_parts} 服务端已落块，复用")
                        result = dict(landed)
                        result.setdefault("partNo", part_no)
                        return result

                backoff = min(retry_count * retry_count, 30)
                logger.warning(f"  块 {part_no} 上传失败: {e}，{backoff}s 后第 {retry_count} 次重试")
                await asyncio.sleep(backoff)

    # ===========================================
    # 创建文件记录（含校验）— 对标 upload.go 的 createFileOnUploadSuccess
    # ===========================================

    async def _create_file_record(self, session: aiohttp.ClientSession,
                                   name: str, upload_id: str, path: str,
                                   uploaded_parts: List[Dict],
                                   total_size: int,
                                   total_parts: int) -> dict:
        """上传完成后校验 parts 并创建文件记录

        同号重复 part（客户端超时重传与服务端异步落库竞态产生）在这里
        做数据级去重自愈：每个 partNo 择优保留一个，落选块作为 orphan_parts
        返回给上层清理其 Telegram 消息；只有真正缺块时才报错。
        """
        # 服务端 parts 列表可能有短暂的写后读延迟；缺块时等待收敛再判定，
        # 避免把"列表尚未更新"误判为污染（会触发全量清理重传）
        remote_parts: List[Dict[str, Any]] = []
        orphan_parts: List[Dict[str, Any]] = []
        for attempt in range(3):
            remote_parts_raw = await self._get_file_parts_with_retry(session, upload_id)
            selected_map, orphan_parts = self._dedupe_remote_parts(
                remote_parts_raw, total_parts, total_size
            )
            remote_parts = [selected_map[number] for number in sorted(selected_map.keys())]
            if len(remote_parts) >= total_parts or attempt == 2:
                break
            logger.warning(
                f"  远端分块列表不完整 ({len(remote_parts)}/{total_parts})，"
                f"等待 5s 后重新校验 ({attempt + 1}/3)"
            )
            await asyncio.sleep(5)
        if orphan_parts:
            logger.warning(
                f"  检测到 {len(orphan_parts)} 个重复/越界分块，已自动去重 "
                f"(保留 {len(remote_parts)}/{total_parts})"
            )

        validation_error = self._validate_remote_parts(remote_parts, total_parts)
        if validation_error:
            validation_error["remote_parts"] = remote_parts
            validation_error["uploaded_parts"] = uploaded_parts
            validation_error["orphan_parts"] = orphan_parts
            return validation_error

        ordered_remote_parts = self._order_remote_parts(remote_parts)

        # 构建 parts 列表（使用远程返回的 partId 和 salt）
        format_parts = []
        for p in ordered_remote_parts:
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
                return {
                    "success": True,
                    "data": result,
                    "remote_parts": ordered_remote_parts,
                    "orphan_parts": orphan_parts,
                }
            else:
                text = await resp.text()
                return {
                    "success": False,
                    "error": f"创建文件记录失败 (HTTP {resp.status}): {text}",
                    "orphan_parts": orphan_parts,
                }

    # ===========================================
    # 串行上传 — 对标 upload.go 的 doSingleUpload
    # ===========================================

    async def _do_single_upload(self, session: aiohttp.ClientSession,
                                 file_path: Path, upload_id: str,
                                 filename: str, file_size: int,
                                 total_parts: int,
                                 progress_callback: Optional[Callable],
                                 confirmed_part_numbers: Optional[List[int]] = None,
                                 remote_parts: Optional[List[Dict[str, Any]]] = None,
                                 part_confirm_callback: Optional[Callable] = None) -> List[Dict]:
        """串行逐块上传，按 TelDrive 已确认的 part 累计进度。"""
        parts = []
        offset = 0
        part_no = 1
        confirmed_bytes = 0
        confirmed_set = set(self._normalize_confirmed_part_numbers(confirmed_part_numbers, total_parts))
        remote_parts_map = self._build_remote_parts_map(remote_parts, total_parts)

        # 复核已确认分块：尺寸不符（chunk_size 变更/污染）的从复用集合剔除，强制重传
        for confirmed_no in list(confirmed_set):
            expected_size = self._expected_part_size(file_size, confirmed_no, total_parts)
            cached = remote_parts_map.get(confirmed_no)
            if cached is not None and not self._part_size_matches(cached, expected_size):
                logger.warning(
                    f"  已确认块 {confirmed_no} 尺寸与当前分块方案不符 "
                    f"(expected={expected_size}, actual={self._get_part_size(cached)})，将重新上传"
                )
                remote_parts_map.pop(confirmed_no, None)
                confirmed_set.discard(confirmed_no)
                continue
            if confirmed_no not in remote_parts_map:
                existing = await self._check_part_exists(
                    session, upload_id, confirmed_no, remote_parts_map, expected_size
                )
                if existing and self._get_part_message_id(existing) is not None:
                    remote_parts_map[confirmed_no] = dict(existing)
                else:
                    confirmed_set.discard(confirmed_no)

        while offset < file_size:
            cur_chunk_size = min(self.chunk_size, file_size - offset)
            logger.info(f"  上传块 {part_no}/{total_parts} ({cur_chunk_size} bytes)")

            if part_no in confirmed_set and part_no in remote_parts_map:
                part_result = dict(remote_parts_map[part_no])
            else:
                part_result = await self._upload_single_chunk(
                    session, upload_id, file_path,
                    chunk_offset=offset,
                    chunk_size=cur_chunk_size,
                    part_no=part_no, filename=filename, total_parts=total_parts,
                )
                remote_parts_map[part_no] = dict(part_result)
            parts.append(part_result)
            confirmed_set.add(part_no)
            confirmed_bytes += cur_chunk_size
            if progress_callback:
                await progress_callback(min(confirmed_bytes, file_size), file_size, len(confirmed_set), total_parts)
            if part_confirm_callback:
                await part_confirm_callback(
                    part_no,
                    dict(part_result),
                    self._normalize_confirmed_part_numbers(confirmed_set, total_parts),
                    [dict(remote_parts_map[number]) for number in sorted(remote_parts_map.keys())],
                    total_parts,
                )
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
        """并发分块上传，按 TelDrive 已确认的 part 累计进度。

        内存占用: upload_concurrency × STREAM_BLOCK(1MB) ≈ 4MB（而非旧版 ~2GB）
        """
        sem = asyncio.Semaphore(self.upload_concurrency)
        results: Dict[int, Dict] = {}
        lock = asyncio.Lock()
        confirmed_bytes = 0
        confirmed_parts = 0

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
            nonlocal confirmed_bytes, confirmed_parts
            async with sem:

                logger.info(f"  并发上传块 {p_no}/{total_parts} ({p_size} bytes)")
                part_result = await self._upload_single_chunk(
                    session, upload_id, file_path,
                    chunk_offset=p_offset,
                    chunk_size=p_size,
                    part_no=p_no, filename=filename, total_parts=total_parts,
                )
                async with lock:
                    results[p_no] = part_result
                    confirmed_bytes += p_size
                    confirmed_parts += 1
                    if progress_callback:
                        await progress_callback(min(confirmed_bytes, file_size), file_size, confirmed_parts, total_parts)


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
                                   progress_callback: Callable = None,
                                   *,
                                   upload_id: Optional[str] = None,
                                   confirmed_part_numbers: Optional[List[int]] = None,
                                   remote_parts: Optional[List[Dict[str, Any]]] = None,
                                   part_confirm_callback: Optional[Callable] = None) -> dict:
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
            progress_callback: 进度回调函数 (confirmed_uploaded_bytes, total_bytes, confirmed_parts, total_parts)



        Returns:
            上传结果 dict
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_size = file_path.stat().st_size
        filename = file_path.name
        upload_id = str(upload_id or uuid.uuid4())

        total_parts = int(math.ceil(file_size / self.chunk_size)) if file_size > 0 else 0
        confirmed_numbers = self._normalize_confirmed_part_numbers(confirmed_part_numbers, total_parts)
        upload_meta = {
            "upload_id": upload_id,
            "total_parts": total_parts,
            "confirmed_part_numbers": confirmed_numbers,
            "uploaded_parts": [],
            "remote_parts": self._normalize_remote_parts(remote_parts, total_parts),
            "orphan_parts": [],
        }

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
        should_cleanup_upload = False
        async with aiohttp.ClientSession(timeout=upload_session_timeout) as session:
            try:
                # 步骤 1: 查找并删除同名文件（对标 driver.go Put 中的逻辑）
                if not confirmed_numbers:
                    existing_file = await self._find_file(session, teldrive_path, filename)
                    if existing_file:
                        file_id = existing_file.get("id")
                        if file_id:
                            logger.info(f"发现同名文件 {filename} (id={file_id})，删除后重新上传")
                            await self._delete_file(session, file_id)
                else:
                    # 断点续传场景：若上次重试已成功创建了文件记录（完成态未落库），
                    # 同名同尺寸文件已存在 → 直接判定成功，避免重复上传+重复记录
                    existing_file = await self._find_file(session, teldrive_path, filename)
                    if existing_file and self._coerce_int(existing_file.get("size")) == file_size:
                        logger.info(
                            f"目标路径已存在同名同尺寸文件 {filename} (size={file_size})，"
                            f"判定上次上传已成功，跳过重传"
                        )
                        should_cleanup_upload = True
                        upload_meta["confirmed_part_numbers"] = list(range(1, total_parts + 1))
                        return {
                            "success": True,
                            "data": existing_file,
                            "already_exists": True,
                            "upload_meta": upload_meta,
                        }

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

                remote_parts_raw = await self._get_file_parts_with_retry(session, upload_id)
                selected_map, orphan_parts_live = self._dedupe_remote_parts(
                    remote_parts_raw, total_parts, file_size
                )
                remote_parts_live = [selected_map[number] for number in sorted(selected_map.keys())]
                if orphan_parts_live:
                    logger.warning(
                        f"上传会话 {upload_id} 存在 {len(orphan_parts_live)} 个重复/越界分块，"
                        f"已自动去重并标记孤儿块待清理"
                    )
                upload_meta["remote_parts"] = remote_parts_live
                upload_meta["orphan_parts"] = [dict(part) for part in orphan_parts_live]
                remote_numbers = self._extract_confirmed_part_numbers(remote_parts_live)
                all_confirmed_numbers = self._normalize_confirmed_part_numbers(
                    list(set(confirmed_numbers) | set(remote_numbers)),
                    total_parts,
                )
                upload_meta["confirmed_part_numbers"] = all_confirmed_numbers

                # 步骤 4: 上传分块（保守正确策略：统一串行补缺块）
                uploaded_parts = await self._do_single_upload(
                    session, file_path, upload_id, filename,
                    file_size, total_parts, progress_callback,
                    confirmed_part_numbers=all_confirmed_numbers,
                    remote_parts=remote_parts_live,
                    part_confirm_callback=part_confirm_callback,
                )
                upload_meta["confirmed_part_numbers"] = self._extract_confirmed_part_numbers(uploaded_parts)
                upload_meta["uploaded_parts"] = uploaded_parts

                # 步骤 5: 创建文件记录（含 parts 校验）
                result = await self._create_file_record(
                    session, filename, upload_id, teldrive_path,
                    uploaded_parts, file_size, total_parts
                )
                result["upload_meta"] = upload_meta
                if isinstance(result.get("remote_parts"), list):
                    upload_meta["remote_parts"] = self._normalize_remote_parts(result["remote_parts"], total_parts)
                    upload_meta["confirmed_part_numbers"] = self._extract_confirmed_part_numbers(upload_meta["remote_parts"])
                if isinstance(result.get("orphan_parts"), list) and result["orphan_parts"]:
                    known_orphan_ids = {
                        self._get_part_message_id(part)
                        for part in upload_meta.get("orphan_parts") or []
                    }
                    for part in result["orphan_parts"]:
                        if self._get_part_message_id(part) not in known_orphan_ids:
                            upload_meta.setdefault("orphan_parts", []).append(dict(part))

                if result.get("success"):
                    should_cleanup_upload = True
                    logger.info(f"文件 {filename} 上传成功")
                else:
                    logger.error(f"文件 {filename} 创建记录失败: {result.get('error')}")

                return result

            except Exception as e:
                logger.error(f"上传文件失败: {e}")
                return {"success": False, "error": str(e), "upload_meta": upload_meta}

            finally:
                # 步骤 6: 清理上传记录（对标 driver.go Put 的 defer）
                if should_cleanup_upload:
                    await self._cleanup_upload(session, upload_id)
