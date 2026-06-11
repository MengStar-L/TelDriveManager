"""断点续传一致性（源指纹）测试。

chunk_size 配置变更或本地文件被改写后，已持久化的分块 checkpoint
不再可信，续传前必须丢弃并重新上传，否则会拼出损坏文件。
"""

import os
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast

from app.modules.aria2teldrive import task_manager as task_manager_module


class FakeTelDrive:
    def __init__(self, chunk_size: int):
        self.chunk_size = chunk_size
        self.cleaned_upload_ids = []

    async def cleanup_upload_session(self, upload_id):
        self.cleaned_upload_ids.append(upload_id)


class UploadFingerprintTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.manager = task_manager_module.TaskManager()
        self.manager.teldrive = cast(Any, FakeTelDrive(chunk_size=1024))

    def write_file(self, name: str, content: bytes) -> str:
        path = Path(self.tmpdir.name) / name
        path.write_bytes(content)
        return str(path)

    def patch_db(self, task_store: dict):
        original_update = task_manager_module.db.update_task
        original_get = task_manager_module.db.get_task

        async def fake_update(task_id, **kwargs):
            task_store.update(kwargs)

        async def fake_get(task_id):
            return dict(task_store)

        cast(Any, task_manager_module.db).update_task = fake_update
        cast(Any, task_manager_module.db).get_task = fake_get

        def restore():
            cast(Any, task_manager_module.db).update_task = original_update
            cast(Any, task_manager_module.db).get_task = original_get
        self.addCleanup(restore)

    async def test_matching_fingerprint_keeps_checkpoint(self):
        local_path = self.write_file("a.bin", b"x" * 4096)
        fingerprint = self.manager._calc_upload_source_fingerprint(local_path)
        task = {
            "task_id": "t1",
            "upload_id": "u1",
            "upload_confirmed_chunks": 2.0,
            "upload_confirmed_total": 4,
            "upload_confirmed_parts_json": "[1,2]",
            "upload_remote_parts_json": "[]",
            "upload_source_fingerprint": fingerprint,
        }
        self.patch_db(task)

        result = await self.manager._invalidate_stale_upload_checkpoint("t1", dict(task), local_path)

        self.assertEqual(result.get("upload_id"), "u1")
        self.assertEqual(result.get("upload_confirmed_parts_json"), "[1,2]")
        self.assertEqual(self.manager.teldrive.cleaned_upload_ids, [])

    async def test_chunk_size_change_drops_checkpoint(self):
        local_path = self.write_file("b.bin", b"x" * 4096)
        # 指纹按 chunk_size=1024 生成
        stale_fingerprint = self.manager._calc_upload_source_fingerprint(local_path)
        # 之后 chunk_size 配置变更
        self.manager.teldrive.chunk_size = 2048
        task = {
            "task_id": "t2",
            "upload_id": "u2",
            "upload_confirmed_chunks": 2.0,
            "upload_confirmed_total": 4,
            "upload_confirmed_parts_json": "[1,2]",
            "upload_remote_parts_json": "[]",
            "upload_source_fingerprint": stale_fingerprint,
        }
        self.patch_db(task)

        result = await self.manager._invalidate_stale_upload_checkpoint("t2", dict(task), local_path)

        self.assertIsNone(result.get("upload_id"))
        self.assertEqual(result.get("upload_confirmed_parts_json"), "[]")
        self.assertEqual(result.get("upload_confirmed_chunks"), 0)
        # 旧服务端会话被废弃，防止新旧分块混在同一 upload_id
        self.assertEqual(self.manager.teldrive.cleaned_upload_ids, ["u2"])
        # 新指纹已落库
        self.assertEqual(
            result.get("upload_source_fingerprint"),
            self.manager._calc_upload_source_fingerprint(local_path),
        )

    async def test_file_content_change_drops_checkpoint(self):
        local_path = self.write_file("c.bin", b"x" * 4096)
        stale_fingerprint = self.manager._calc_upload_source_fingerprint(local_path)
        # 文件被改写（大小变化）
        Path(local_path).write_bytes(b"y" * 5000)
        task = {
            "task_id": "t3",
            "upload_id": "u3",
            "upload_confirmed_chunks": 1.0,
            "upload_confirmed_total": 4,
            "upload_confirmed_parts_json": "[1]",
            "upload_remote_parts_json": "[]",
            "upload_source_fingerprint": stale_fingerprint,
        }
        self.patch_db(task)

        result = await self.manager._invalidate_stale_upload_checkpoint("t3", dict(task), local_path)

        self.assertIsNone(result.get("upload_id"))
        self.assertEqual(result.get("upload_confirmed_parts_json"), "[]")

    async def test_no_checkpoint_just_records_fingerprint(self):
        local_path = self.write_file("d.bin", b"x" * 100)
        task = {
            "task_id": "t4",
            "upload_id": None,
            "upload_confirmed_chunks": 0,
            "upload_confirmed_total": 0,
            "upload_confirmed_parts_json": "[]",
            "upload_remote_parts_json": "[]",
            "upload_source_fingerprint": None,
        }
        self.patch_db(task)

        result = await self.manager._invalidate_stale_upload_checkpoint("t4", dict(task), local_path)

        self.assertEqual(
            result.get("upload_source_fingerprint"),
            self.manager._calc_upload_source_fingerprint(local_path),
        )
        self.assertEqual(self.manager.teldrive.cleaned_upload_ids, [])

    async def test_legacy_task_without_fingerprint_drops_checkpoint(self):
        # 旧版本任务无指纹字段：有 checkpoint 但 stored 为空 → 不匹配 → 重置
        # （保守正确：旧 checkpoint 可能产生于损坏的 _read_block 版本）
        local_path = self.write_file("e.bin", b"x" * 4096)
        task = {
            "task_id": "t5",
            "upload_id": "u5",
            "upload_confirmed_chunks": 2.0,
            "upload_confirmed_total": 4,
            "upload_confirmed_parts_json": "[1,2]",
            "upload_remote_parts_json": "[]",
            "upload_source_fingerprint": None,
        }
        self.patch_db(task)

        result = await self.manager._invalidate_stale_upload_checkpoint("t5", dict(task), local_path)

        self.assertIsNone(result.get("upload_id"))
        self.assertEqual(result.get("upload_confirmed_parts_json"), "[]")


if __name__ == "__main__":
    unittest.main()
