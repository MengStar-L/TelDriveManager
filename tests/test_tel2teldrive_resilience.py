import asyncio
import unittest
from types import SimpleNamespace
from typing import Any, cast

from app.modules.tel2teldrive import service as service_module


class FakeActivityLogger:
    def __init__(self):
        self.infos = []
        self.warnings = []
        self.errors = []

    def info(self, message):
        self.infos.append(str(message))

    def warning(self, message):
        self.warnings.append(str(message))

    def error(self, message):
        self.errors.append(str(message))


class FakeBroker:
    def __init__(self):
        self.states = []

    async def update_state(self, **kwargs):
        self.states.append(dict(kwargs))


class Tel2TelDriveResilienceTests(unittest.IsolatedAsyncioTestCase):
    def make_config(self):
        return SimpleNamespace(
            telegram_channel_id=12345,
            sync_interval=0,
            confirm_cycles=2,
            db_enabled=False,
            sync_enabled=True,
        )

    async def test_sync_deletions_keeps_running_after_iteration_failure(self):
        fake_logger = FakeActivityLogger()
        fake_broker = FakeBroker()
        config = self.make_config()
        get_files_calls = 0

        async def fake_run_blocking_io(func, *args, **kwargs):
            nonlocal get_files_calls
            if func is service_module.get_teldrive_files:
                get_files_calls += 1
                if get_files_calls == 1:
                    return {"old-file": {"name": "old.mp4", "size": 1}}
                if get_files_calls == 2:
                    raise RuntimeError("snapshot failed")
                raise asyncio.CancelledError()
            if func is service_module.load_mapping:
                return {}
            if func is service_module.save_mapping:
                return None
            if func is service_module.sync_mapping_from_db:
                return 0
            return func(*args, **kwargs)

        original_logger = service_module.logger
        original_broker = service_module.broker
        original_run_blocking_io = service_module.run_blocking_io
        try:
            service_module.logger = cast(Any, fake_logger)
            service_module.broker = cast(Any, fake_broker)
            service_module.run_blocking_io = cast(Any, fake_run_blocking_io)

            with self.assertRaises(asyncio.CancelledError):
                await service_module.sync_deletions(cast(Any, object()), config)
        finally:
            service_module.logger = original_logger
            service_module.broker = original_broker
            service_module.run_blocking_io = original_run_blocking_io

        self.assertGreaterEqual(get_files_calls, 3)
        self.assertTrue(any("删除同步循环异常" in message for message in fake_logger.errors))
        self.assertTrue(fake_broker.states)

    async def test_sync_task_failure_schedules_restart(self):
        manager = service_module.Tel2TelDriveService()
        fake_logger = FakeActivityLogger()
        restart_calls = []

        async def fail():
            raise RuntimeError("sync crashed")

        async def fake_restart(delay=5.0):
            restart_calls.append(delay)

        original_logger = service_module.logger
        try:
            service_module.logger = cast(Any, fake_logger)
            manager._restart_sync_deletions_after_delay = cast(Any, fake_restart)
            task = asyncio.create_task(fail())
            await asyncio.sleep(0)

            manager.sync_task = task
            manager._on_sync_task_done(task)
            await asyncio.sleep(0)
        finally:
            service_module.logger = original_logger
            if manager.sync_restart_task and not manager.sync_restart_task.done():
                manager.sync_restart_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await manager.sync_restart_task

        self.assertEqual(restart_calls, [5.0])
        self.assertTrue(any("删除同步任务异常退出" in message for message in fake_logger.errors))


if __name__ == "__main__":
    unittest.main()
