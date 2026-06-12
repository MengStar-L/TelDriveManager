import json
import unittest
from typing import Any, cast

from app.modules.aria2teldrive import task_manager as task_manager_module

from tests.test_serial_gate import FakeAria2


class ParallelDispatchTests(unittest.IsolatedAsyncioTestCase):
    def make_manager(self, serial_mode=False, download_dir="."):
        manager = task_manager_module.TaskManager()
        manager.config = {
            "upload": {"serial_transfer_mode": serial_mode, "auto_delete": True, "max_retries": 3},
            "aria2": {
                "max_concurrent": 3,
                "disk_protection_threshold_gb": 1,
                "download_dir": download_dir,
            },
            "teldrive": {
                "upload_concurrency": 4,
                "upload_dir": "",
                "target_path": "/",
                "chunk_size": "500M",
            },
        }
        manager._disk_usage_info = {"free": 10 * 1024 ** 3}
        manager.aria2 = cast(Any, FakeAria2())
        return manager

    def queued_task(self, suffix: str) -> dict:
        return {
            "task_id": f"queued-{suffix}",
            "status": "pending",
            "url": f"https://example.test/{suffix}.bin",
            "filename": f"{suffix}.bin",
            "aria2_gid": None,
            "aria2_options_json": json.dumps({"dir": ".", "out": f"{suffix}.bin"}),
        }

    async def run_dispatch(self, manager, queued):
        updates_by_task = {}

        original_get_queued = task_manager_module.db.get_pending_queued_tasks
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_queued(limit=50):
                return list(queued)

            async def fake_update_task(task_id, **kwargs):
                updates_by_task.setdefault(task_id, {}).update(kwargs)

            async def fake_broadcast(*args, **kwargs):
                return None

            cast(Any, task_manager_module.db).get_pending_queued_tasks = fake_get_queued
            cast(Any, task_manager_module.db).update_task = fake_update_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)

            released = await manager._dispatch_queued_parallel_downloads()
        finally:
            cast(Any, task_manager_module.db).get_pending_queued_tasks = original_get_queued
            cast(Any, task_manager_module.db).update_task = original_update_task

        return released, updates_by_task

    async def test_dispatch_releases_all_db_queued_tasks_in_parallel_mode(self):
        manager = self.make_manager(serial_mode=False)
        queued = [self.queued_task("one"), self.queued_task("two"), self.queued_task("three")]

        released, updates = await self.run_dispatch(manager, queued)

        self.assertEqual(released, 3)
        self.assertEqual(len(manager.aria2.added), 3)
        for task in queued:
            self.assertEqual(updates[task["task_id"]]["status"], "downloading")
            self.assertTrue(updates[task["task_id"]]["aria2_gid"])
        submitted_urls = [url for url, _opts in manager.aria2.added]
        self.assertEqual(submitted_urls, [t["url"] for t in queued])

    async def test_dispatch_noop_in_serial_mode(self):
        manager = self.make_manager(serial_mode=True)
        queued = [self.queued_task("one")]

        released, updates = await self.run_dispatch(manager, queued)

        self.assertEqual(released, 0)
        self.assertEqual(manager.aria2.added, [])
        self.assertEqual(updates, {})

    async def test_dispatch_defers_to_disk_gate_when_protection_active(self):
        manager = self.make_manager(serial_mode=False)
        manager._disk_protection_active = True
        queued = [self.queued_task("one")]

        released, updates = await self.run_dispatch(manager, queued)

        self.assertEqual(released, 1)
        self.assertEqual(len(manager.aria2.added), 1)
        _url, options = manager.aria2.added[0]
        self.assertEqual(options.get("pause"), "true")
        self.assertEqual(updates["queued-one"]["status"], "pending")
        self.assertIn("gid-1", manager._disk_gate_paused_gids)

    async def test_dispatch_marks_task_failed_when_url_missing(self):
        manager = self.make_manager(serial_mode=False)
        task = self.queued_task("one")
        task["url"] = ""

        released, updates = await self.run_dispatch(manager, [task])

        self.assertEqual(released, 0)
        self.assertEqual(manager.aria2.added, [])
        self.assertEqual(updates["queued-one"]["status"], "failed")

    async def test_dispatch_keeps_task_pending_when_aria2_rejects(self):
        manager = self.make_manager(serial_mode=False)

        async def failing_add_uri(url, options=None):
            raise RuntimeError("aria2 unavailable")

        manager.aria2.add_uri = cast(Any, failing_add_uri)

        released, updates = await self.run_dispatch(manager, [self.queued_task("one")])

        self.assertEqual(released, 0)
        self.assertEqual(updates["queued-one"]["status"], "pending")
        self.assertIn("aria2 unavailable", updates["queued-one"]["error"])


if __name__ == "__main__":
    unittest.main()
