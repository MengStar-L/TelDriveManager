"""tel2teldrive 误删防护测试。

覆盖三条曾导致"刚上传成功的文件被自动删除/分块消息被误删"的链条：
1. delete_teldrive_files_for_missing_messages 在本地映射过期时，
   应以数据库权威 parts 刷新映射而不是删除文件；
2. 同名覆盖（_delete_file）须通知 remember_internal_deleted_file_ids；
3. 重复 part 去重（_dedupe_remote_parts）择优保留、孤儿外溢。
"""

import unittest
from typing import Any, cast

from app.modules.aria2teldrive.teldrive_client import TelDriveClient
from app.modules.tel2teldrive import service as service_module


class DedupeRemotePartsTests(unittest.TestCase):
    def make_client(self, chunk_size: int = 100) -> TelDriveClient:
        client = TelDriveClient()
        client.chunk_size = chunk_size
        return client

    def test_duplicate_part_numbers_keep_latest_matching_size(self):
        client = self.make_client(chunk_size=100)
        # 文件 250 字节 → 3 块：100/100/50
        selected, orphans = client._dedupe_remote_parts(
            [
                {"partNo": 1, "partId": 11, "size": 100},
                {"partNo": 1, "partId": 15, "size": 100},  # 重复，partId 更大 → 保留
                {"partNo": 2, "partId": 12, "size": 100},
                {"partNo": 3, "partId": 13, "size": 50},
            ],
            total_parts=3,
            file_size=250,
        )
        self.assertEqual(sorted(selected.keys()), [1, 2, 3])
        self.assertEqual(selected[1]["partId"], 15)
        self.assertEqual([p["partId"] for p in orphans], [11])

    def test_size_mismatch_part_is_orphaned(self):
        client = self.make_client(chunk_size=100)
        selected, orphans = client._dedupe_remote_parts(
            [
                {"partNo": 1, "partId": 11, "size": 100},
                # 旧 chunk_size 留下的错位块（尺寸不符但 partId 更大）
                {"partNo": 1, "partId": 99, "size": 64},
                {"partNo": 2, "partId": 12, "size": 50},
            ],
            total_parts=2,
            file_size=150,
        )
        self.assertEqual(selected[1]["partId"], 11)
        self.assertEqual([p["partId"] for p in orphans], [99])

    def test_out_of_range_part_is_orphaned(self):
        client = self.make_client(chunk_size=100)
        selected, orphans = client._dedupe_remote_parts(
            [
                {"partNo": 1, "partId": 11, "size": 100},
                {"partNo": 7, "partId": 77, "size": 100},  # 越界
            ],
            total_parts=1,
            file_size=100,
        )
        self.assertEqual(sorted(selected.keys()), [1])
        self.assertEqual([p["partId"] for p in orphans], [77])

    def test_parts_without_size_are_accepted(self):
        # 远端未提供 size 字段时不应误杀
        client = self.make_client(chunk_size=100)
        selected, orphans = client._dedupe_remote_parts(
            [{"partNo": 1, "partId": 11}, {"partNo": 2, "partId": 12}],
            total_parts=2,
            file_size=150,
        )
        self.assertEqual(sorted(selected.keys()), [1, 2])
        self.assertEqual(orphans, [])


class InternalDeleteNotificationTests(unittest.TestCase):
    def test_notify_internal_file_deletion_registers_grace(self):
        TelDriveClient._notify_internal_file_deletion("file-overwrite-1")
        self.assertTrue(
            service_module.consume_internal_deleted_file_id("file-overwrite-1")
        )
        # 消费后即失效
        self.assertFalse(
            service_module.consume_internal_deleted_file_id("file-overwrite-1")
        )


class MissingMessageDeletionGuardTests(unittest.IsolatedAsyncioTestCase):
    def make_config(self, db_enabled: bool):
        from types import SimpleNamespace
        return SimpleNamespace(
            telegram_channel_id=12345,
            db_enabled=db_enabled,
            teldrive_url="http://localhost",
            bearer_token="t",
            log_buffer_size=100,
        )

    async def _run_with_fakes(self, config, mapping, td_files, db_mapping,
                              missing_ids, foreign_file_ids=None):
        deleted_files = []
        saved_mappings = []

        async def fake_run_blocking_io(func, *args, **kwargs):
            if func is service_module.load_mapping:
                return dict(mapping)
            if func is service_module.save_mapping:
                saved_mappings.append(dict(args[0]))
                return None
            if func is service_module.query_db_mapping:
                return dict(db_mapping)
            if func is service_module.query_db_foreign_file_ids:
                return set(foreign_file_ids or ())
            if func is service_module.get_teldrive_files:
                return dict(td_files)
            return func(*args, **kwargs)

        async def fake_delete_file(config_, **kwargs):
            deleted_files.append(kwargs["file_id"])
            return True

        original_run_blocking_io = service_module.run_blocking_io
        original_delete = service_module.delete_file_from_teldrive
        try:
            service_module.run_blocking_io = cast(Any, fake_run_blocking_io)
            service_module.delete_file_from_teldrive = cast(Any, fake_delete_file)
            deleted_count = await service_module.delete_teldrive_files_for_missing_messages(
                config, missing_ids, td_files=dict(td_files),
            )
        finally:
            service_module.run_blocking_io = original_run_blocking_io
            service_module.delete_file_from_teldrive = original_delete
        return deleted_count, deleted_files, saved_mappings

    async def test_foreign_channel_file_is_never_deleted(self):
        # 分块存储在其他频道的文件：监听频道里查不到其消息 ID 属正常，
        # 绝不能据此删除；应从映射剔除并排除出删除同步
        config = self.make_config(db_enabled=True)
        deleted_count, deleted_files, saved_mappings = await self._run_with_fakes(
            config,
            mapping={"file-foreign": [301, 302]},
            td_files={"file-foreign": {"name": "other-channel.mkv", "size": 10}},
            db_mapping={},  # 频道过滤后 db_mapping 不含外频道文件
            missing_ids=[301, 302],
            foreign_file_ids={"file-foreign"},
        )
        self.assertEqual(deleted_count, 0)
        self.assertEqual(deleted_files, [])
        self.assertTrue(saved_mappings)
        self.assertNotIn("file-foreign", saved_mappings[-1])


class ChannelIdMatchTests(unittest.TestCase):
    def test_equivalent_forms_match(self):
        # TelDrive DB 存裸 ID，Telethon 配置常带 -100 前缀
        self.assertTrue(service_module.channel_ids_match(3854656012, -1003854656012))
        self.assertTrue(service_module.channel_ids_match("-1003854656012", "3854656012"))
        self.assertTrue(service_module.channel_ids_match(1003854656012, 3854656012))

    def test_different_channels_do_not_match(self):
        self.assertFalse(service_module.channel_ids_match(3854656012, -10038190483))

    def test_invalid_or_zero_never_match(self):
        self.assertFalse(service_module.channel_ids_match(0, 3854656012))
        self.assertFalse(service_module.channel_ids_match(None, 3854656012))
        self.assertFalse(service_module.channel_ids_match("abc", 3854656012))

    def test_short_ids_do_not_strip_100_prefix(self):
        # 短 ID 不剥 100 前缀，避免 100123 与 123 误判等价
        self.assertFalse(service_module.channel_ids_match(100123, 123))

    async def test_stale_mapping_is_refreshed_instead_of_deleting(self):
        # 本地映射记录旧 msg_id（已缺失），但数据库权威 parts 显示
        # 文件实际由新消息组成 → 应刷新映射、不删文件
        config = self.make_config(db_enabled=True)
        deleted_count, deleted_files, saved_mappings = await self._run_with_fakes(
            config,
            mapping={"file-1": [101, 102]},        # 过期映射
            td_files={"file-1": {"name": "movie.mkv", "size": 10}},
            db_mapping={"file-1": [201, 202]},     # 权威 parts（全部存在）
            missing_ids=[101, 102],
        )
        self.assertEqual(deleted_count, 0)
        self.assertEqual(deleted_files, [])
        self.assertTrue(saved_mappings)
        self.assertEqual(saved_mappings[-1]["file-1"], [201, 202])

    async def test_authoritative_missing_part_still_deletes(self):
        # 权威 parts 中确有缺失 → 仍然删除（保持原有保护行为）
        config = self.make_config(db_enabled=True)
        deleted_count, deleted_files, _ = await self._run_with_fakes(
            config,
            mapping={"file-1": [201, 202]},
            td_files={"file-1": {"name": "movie.mkv", "size": 10}},
            db_mapping={"file-1": [201, 202]},
            missing_ids=[202],
        )
        self.assertEqual(deleted_count, 1)
        self.assertEqual(deleted_files, ["file-1"])

    async def test_no_db_falls_back_to_tracked_mapping(self):
        # 未启用数据库时维持旧行为：按本地映射判定
        config = self.make_config(db_enabled=False)
        deleted_count, deleted_files, _ = await self._run_with_fakes(
            config,
            mapping={"file-1": [101]},
            td_files={"file-1": {"name": "movie.mkv", "size": 10}},
            db_mapping={},
            missing_ids=[101],
        )
        self.assertEqual(deleted_count, 1)
        self.assertEqual(deleted_files, ["file-1"])


if __name__ == "__main__":
    unittest.main()
