"""上传字节一致性集成测试 — 用本地 mock TelDrive 服务器接收上传并逐字节比对。

回归保护：_read_block 曾用 bytearray 切片（独立拷贝）调用 readinto，
导致每个 chunk 的最后一个不足 1MB 的尾块发送的是上一块的旧数据（静默损坏）。
"""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path

from aiohttp import web

from app.modules.aria2teldrive.teldrive_client import TelDriveClient

MB = 1024 * 1024


class MockTelDriveServer:
    """最小 TelDrive API mock：记录上传的每个 part 的原始字节。"""

    def __init__(self):
        # upload_id -> {part_no: bytes}
        self.uploads: dict[str, dict[int, bytes]] = {}
        self.created_files: list[dict] = []
        self.deleted_upload_ids: list[str] = []
        # 可注入故障：part_no -> 失败次数（消耗后恢复正常）
        self.fail_part_once: dict[int, int] = {}
        self.runner = None
        self.port = None

    def _parts_list(self, upload_id: str) -> list[dict]:
        parts = self.uploads.get(upload_id, {})
        return [
            {"name": f"part{no}", "partId": 1000 + no, "partNo": no, "size": len(data)}
            for no, data in sorted(parts.items())
        ]

    async def handle_get_upload(self, request: web.Request):
        upload_id = request.match_info["upload_id"]
        return web.json_response(self._parts_list(upload_id))

    async def handle_post_upload(self, request: web.Request):
        upload_id = request.match_info["upload_id"]
        part_no = int(request.query["partNo"])
        body = await request.read()
        remaining_failures = self.fail_part_once.get(part_no, 0)
        if remaining_failures > 0:
            self.fail_part_once[part_no] = remaining_failures - 1
            return web.json_response({"message": "injected failure"}, status=500)
        self.uploads.setdefault(upload_id, {})[part_no] = body
        return web.json_response(
            {"name": request.query.get("partName", f"part{part_no}"),
             "partId": 1000 + part_no, "partNo": part_no, "size": len(body)}
        )

    async def handle_delete_upload(self, request: web.Request):
        self.deleted_upload_ids.append(request.match_info["upload_id"])
        return web.Response(status=204)

    async def handle_find_files(self, request: web.Request):
        return web.json_response({"items": []})

    async def handle_create_file(self, request: web.Request):
        payload = await request.json()
        self.created_files.append(payload)
        return web.json_response({"id": "file-1", "name": payload.get("name")})

    async def handle_mkdir(self, request: web.Request):
        return web.Response(status=204)

    async def start(self):
        app = web.Application(client_max_size=64 * MB)
        app.router.add_get("/api/uploads/{upload_id}", lambda r: self.handle_get_upload(r))
        app.router.add_post("/api/uploads/{upload_id}", lambda r: self.handle_post_upload(r))
        app.router.add_delete("/api/uploads/{upload_id}", lambda r: self.handle_delete_upload(r))
        app.router.add_get("/api/files", lambda r: self.handle_find_files(r))
        app.router.add_post("/api/files", lambda r: self.handle_create_file(r))
        app.router.add_post("/api/files/mkdir", lambda r: self.handle_mkdir(r))
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    def reassembled(self, upload_id: str) -> bytes:
        parts = self.uploads.get(upload_id, {})
        return b"".join(data for _, data in sorted(parts.items()))


def make_test_payload(size: int) -> bytes:
    """生成无周期性的测试数据，确保错位/旧块复用都会被比对发现。"""
    chunk = os.urandom(min(size, 64 * 1024))
    repeats = size // len(chunk) + 1
    data = bytearray((chunk * repeats)[:size])
    # 写入位置指纹，破坏 urandom 块重复带来的周期性
    for offset in range(0, size, 4096):
        marker = offset.to_bytes(8, "little")
        data[offset:offset + min(8, size - offset)] = marker[: min(8, size - offset)]
    return bytes(data)


class UploadIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = MockTelDriveServer()
        await self.server.start()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self.server.stop)
        self.addCleanup(self.tmpdir.cleanup)

    def make_client(self, chunk_size_bytes: int) -> TelDriveClient:
        client = TelDriveClient(
            api_host=f"http://127.0.0.1:{self.server.port}",
            access_token="test-token",
            chunk_size="500M",
            upload_concurrency=2,
            max_retries=2,
        )
        client.chunk_size = chunk_size_bytes
        # 测试环境不需要等待服务端异步落块
        client.AMBIGUOUS_POLL_FLOOR_SECONDS = 1
        return client

    def write_file(self, name: str, payload: bytes) -> str:
        path = Path(self.tmpdir.name) / name
        path.write_bytes(payload)
        return str(path)

    async def _upload_and_verify(self, payload: bytes, chunk_size: int,
                                 expected_parts: int):
        client = self.make_client(chunk_size)
        file_path = self.write_file(f"f_{len(payload)}.bin", payload)
        upload_id = f"upload-{len(payload)}-{chunk_size}"
        result = await client.upload_file_chunked(
            file_path, "/", upload_id=upload_id
        )
        self.assertTrue(result.get("success"), msg=f"upload failed: {result}")
        parts = self.server.uploads.get(upload_id, {})
        self.assertEqual(len(parts), expected_parts)
        self.assertEqual(
            sorted(parts.keys()), list(range(1, expected_parts + 1)))
        reassembled = self.server.reassembled(upload_id)
        self.assertEqual(len(reassembled), len(payload))
        self.assertEqual(reassembled, payload, "上传内容与源文件不一致（数据损坏）")
        # 创建文件记录的 parts 顺序必须与 partNo 一致
        self.assertEqual(len(self.server.created_files), 1)
        record = self.server.created_files[0]
        self.assertEqual(record["size"], len(payload))
        self.assertEqual(
            [p["id"] for p in record["parts"]],
            [1000 + n for n in range(1, expected_parts + 1)],
        )

    async def test_small_file_below_stream_block(self):
        # < 1MB：旧 bug 下整个文件内容损坏（全为 buf 初始零值）
        payload = make_test_payload(512 * 1024 + 7)
        await self._upload_and_verify(payload, chunk_size=2 * MB, expected_parts=1)

    async def test_multi_chunk_with_partial_tail(self):
        # 跨 chunk 且尾部不足 1MB：旧 bug 下尾块内容是上一 1MB 块的旧数据
        payload = make_test_payload(2 * MB + 700 * 1024 + 13)
        await self._upload_and_verify(payload, chunk_size=MB, expected_parts=3)

    async def test_exact_multiple_of_stream_block(self):
        # 整 MB 文件：所有块都是完整 STREAM_BLOCK
        payload = make_test_payload(3 * MB)
        await self._upload_and_verify(payload, chunk_size=MB, expected_parts=3)

    async def test_chunk_retry_after_transient_500_keeps_integrity(self):
        # 单块瞬时失败 → 重试后内容仍逐字节一致
        self.server.fail_part_once[2] = 1
        payload = make_test_payload(2 * MB + 321)
        await self._upload_and_verify(payload, chunk_size=MB, expected_parts=3)

    async def test_truncated_file_raises_instead_of_silent_short_part(self):
        # 上传中文件被截断：必须显式失败，绝不提交字节数不足的 part
        payload = make_test_payload(2 * MB)
        client = self.make_client(MB)
        file_path = self.write_file("truncated.bin", payload)

        original_post = self.server.handle_post_upload
        truncated = asyncio.Event()

        async def truncate_then_post(request):
            if not truncated.is_set():
                truncated.set()
                Path(file_path).write_bytes(payload[: MB // 2])
            return await original_post(request)

        self.server.handle_post_upload = truncate_then_post
        # 截断发生在 server 端收到请求时，客户端读文件在请求体流式发送阶段，
        # 直接调用底层接口验证 EOF 防护
        result = await client.upload_file_chunked(
            file_path, "/", upload_id="upload-truncated"
        )
        # 核心断言：上传必须整体失败（EOF 防护在 data_sender 内抛错，
        # aiohttp 会包装为 "Can not write request body"），且 server 端
        # 绝不能存在字节数不足 chunk_size 的 part（旧行为是静默 break 提交短块）
        self.assertFalse(result.get("success"), "截断文件的上传不应整体成功")
        for part_no, data in self.server.uploads.get("upload-truncated", {}).items():
            self.assertEqual(
                len(data), MB,
                f"part {part_no} 字节数不足却被提交（短块泄漏）",
            )
        self.assertEqual(len(self.server.created_files), 0)

    async def test_duplicate_parts_on_server_are_deduped_not_polluted(self):
        # 服务端已有同号重复 part（超时重传竞态的产物）：
        # 旧行为整体判污染 → 清空会话重传；新行为应择优去重并成功建档
        payload = make_test_payload(2 * MB + 123)
        client = self.make_client(MB)
        file_path = self.write_file("dup.bin", payload)
        upload_id = "upload-dup"

        # 预置：part 2 有两份（partId 不同，内容相同），其中一份为陈旧短块
        chunk2 = payload[MB:2 * MB]
        self.server.uploads[upload_id] = {}

        # 直接注入重复响应：mock parts 列表返回重复编号
        original_get = self.server.handle_get_upload
        injected = {"done": False}

        async def get_with_duplicates(request):
            from aiohttp import web as _web
            parts = self.server._parts_list(upload_id)
            if not injected["done"] and any(p["partNo"] == 2 for p in parts):
                # 第一份 partId 较小且尺寸不符（错位旧块）→ 应被去重淘汰
                parts.append({"name": "stale", "partId": 900, "partNo": 2, "size": 77})
            return _web.json_response(parts)

        self.server.handle_get_upload = get_with_duplicates

        result = await client.upload_file_chunked(file_path, "/", upload_id=upload_id)

        self.assertTrue(result.get("success"), msg=f"upload failed: {result}")
        # 建档使用的 partId 应为真实上传的（1000+partNo），不含 stale 900
        record = self.server.created_files[-1]
        part_ids = [p["id"] for p in record["parts"]]
        self.assertNotIn(900, part_ids)
        self.assertEqual(part_ids, [1001, 1002, 1003])
        # 孤儿块外溢给上层清理
        orphan_ids = [
            p.get("partId") for p in result.get("upload_meta", {}).get("orphan_parts", [])
        ]
        self.assertIn(900, orphan_ids)
        self.assertEqual(self.server.reassembled(upload_id), payload)

    async def test_resume_skips_confirmed_parts_and_completes(self):
        # 断点续传：part 1 已在服务端 → 只补传缺失块，内容仍逐字节一致
        payload = make_test_payload(3 * MB + 17)
        client = self.make_client(MB)
        file_path = self.write_file("resume.bin", payload)
        upload_id = "upload-resume"
        self.server.uploads[upload_id] = {1: payload[:MB]}

        uploaded_before = dict(self.server.uploads[upload_id])
        result = await client.upload_file_chunked(
            file_path, "/", upload_id=upload_id,
            confirmed_part_numbers=[1],
            remote_parts=[{"partNo": 1, "partId": 1001, "size": MB}],
        )
        self.assertTrue(result.get("success"), msg=f"upload failed: {result}")
        # part 1 未被重传（字节对象不变）
        self.assertIs(self.server.uploads[upload_id][1], uploaded_before[1])
        self.assertEqual(self.server.reassembled(upload_id), payload)


if __name__ == "__main__":
    unittest.main()
