"""build_initial_mapping 误报防护测试。

频道「名字扫描兜底」对 TelDrive 文件天生匹配不上——分块消息带的是分块名
（随机名/.part/md5），不是 TelDrive 列表里的原始显示名。因此名字扫描扫到 0
是常态。本测试锁定：已由数据库/历史映射覆盖的文件，绝不能因名字扫描复现不出
就被误报“未找到对应 Telegram 消息”；而真正没有任何映射的文件仍要如实告警。
"""

import unittest
from types import SimpleNamespace
from typing import Any, cast

from app.modules.tel2teldrive import service as service_module


def _empty_async_iter(*_args, **_kwargs):
    async def _gen():
        return
        yield  # noqa: 让函数成为空异步生成器
    return _gen()


class RecordingLogger:
    """记录所有级别日志，便于断言是否打出/未打出某条消息。"""

    def __init__(self):
        self.records: list[tuple[str, str]] = []

    def __getattr__(self, level: str):
        def _log(msg: Any, *_a, **_k):
            self.records.append((level.upper(), str(msg)))
        return _log

    def messages(self, level: str) -> list[str]:
        want = level.upper()
        return [m for lvl, m in self.records if lvl == want]


class FakeClient:
    """频道里没有任何能按文件名匹配上的消息（模拟 TelDrive 分块名 != 原名）。"""

    def iter_messages(self, *_a, **_k):
        return _empty_async_iter()


class BuildInitialMappingTests(unittest.IsolatedAsyncioTestCase):
    def make_config(self) -> Any:
        # db_enabled=False：跳过数据库同步分支，直接走频道名字扫描兜底，隔离被测逻辑
        return SimpleNamespace(
            telegram_channel_id=12345,
            db_enabled=False,
            teldrive_url="http://localhost",
            bearer_token="t",
            max_scan_messages=100,
            log_buffer_size=100,
        )

    async def _run(self, td_files: dict, local_mapping: dict):
        saved: list[dict] = []

        async def fake_run_blocking_io(func, *args, **kwargs):
            if func is service_module.get_teldrive_files:
                return dict(td_files)
            if func is service_module.load_mapping:
                return dict(local_mapping)
            if func is service_module.merge_and_save_mapping_snapshot:
                _td, snapshot = args
                merged = service_module.normalize_mapping(snapshot)
                saved.append(merged)
                return merged
            return func(*args, **kwargs)

        logger = RecordingLogger()
        original_run = service_module.run_blocking_io
        original_logger = service_module.logger
        try:
            service_module.run_blocking_io = cast(Any, fake_run_blocking_io)
            service_module.logger = cast(Any, logger)
            await service_module.build_initial_mapping(FakeClient(), self.make_config())
        finally:
            service_module.run_blocking_io = original_run
            service_module.logger = original_logger
        return logger, saved

    async def test_db_mapped_file_not_reported_missing_when_name_scan_blank(self):
        # 文件已有有效映射（来自数据库/历史），名字扫描复现不出 → 不应误报“未找到”
        logger, saved = await self._run(
            td_files={"file-1": {"name": "movie.mkv", "size": 10}},
            local_mapping={"file-1": [201, 202]},
        )
        warns = logger.messages("WARNING")
        self.assertFalse(
            any("未找到对应 Telegram 消息" in m for m in warns),
            f"已有映射的文件不应被误报未找到: {warns}",
        )
        self.assertTrue(
            any("匹配到 1 个文件，未找到 0 个" in m for m in logger.messages("INFO")),
            f"应统计为已匹配: {logger.messages('INFO')}",
        )
        # 已有映射应原样保留
        self.assertEqual(saved[-1].get("file-1"), [201, 202])

    async def test_unmapped_file_still_reported_missing(self):
        # 完全没有映射、名字扫描也扫不到 → 仍要如实告警（保留真实缺失的可见性）
        logger, _saved = await self._run(
            td_files={"file-2": {"name": "ghost.mkv", "size": 10}},
            local_mapping={},
        )
        self.assertTrue(
            any("仍有 1 个" in m for m in logger.messages("WARNING")),
            f"真正无映射的文件应如实告警: {logger.messages('WARNING')}",
        )

    async def test_md5_file_skipped_alongside_mapped_file(self):
        # 混合场景：md5 分片记录在循环里被 continue 跳过（不计匹配也不计缺失），
        # 同时已映射的正常文件仍被算作已匹配
        logger, saved = await self._run(
            td_files={
                "file-1": {"name": "movie.mkv", "size": 10},
                "chunk": {"name": "a" * 32, "size": 10},
            },
            local_mapping={"file-1": [201, 202]},
        )
        self.assertTrue(
            any("匹配到 1 个文件，未找到 0 个" in m for m in logger.messages("INFO")),
            f"md5 应跳过、已映射文件应计为匹配: {logger.messages('INFO')}",
        )
        self.assertFalse(any("仍有" in m for m in logger.messages("WARNING")))
        self.assertEqual(saved[-1].get("file-1"), [201, 202])
        self.assertNotIn("chunk", saved[-1])


if __name__ == "__main__":
    unittest.main()
