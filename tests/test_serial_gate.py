import unittest

from app.modules.aria2teldrive import task_manager as task_manager_module


class FakeAria2:
    def __init__(self):
        self.force_paused = []
        self.paused = []
        self.unpaused = []
        self.added = []

    async def force_pause(self, gid):
        self.force_paused.append(gid)
        return gid

    async def pause(self, gid):
        self.paused.append(gid)
        return gid

    async def unpause(self, gid):
        self.unpaused.append(gid)
        return gid

    async def add_uri(self, url, options=None):
        self.added.append((url, dict(options or {})))
        return "new-gid"


class SerialGateTests(unittest.IsolatedAsyncioTestCase):
    def make_manager(self):
        manager = task_manager_module.TaskManager()
        manager.config = {
            "upload": {"serial_transfer_mode": True, "auto_delete": True, "max_retries": 3},
            "aria2": {
                "max_concurrent": 3,
                "disk_protection_threshold_gb": 1,
                "download_dir": ".",
            },
            "teldrive": {
                "upload_concurrency": 4,
                "upload_dir": "",
                "target_path": "/",
                "chunk_size": "500M",
            },
        }
        manager._disk_usage_info = {"free": 3 * 1024 ** 3}
        manager.aria2 = FakeAria2()

        async def no_blockers(stopped=None):
            return False

        manager._has_serial_resume_blockers = no_blockers
        return manager

    async def test_active_download_holds_all_waiting_items(self):
        manager = self.make_manager()

        await manager._sync_serial_transfer_gate(
            active=[{"gid": "active-1", "status": "active"}],
            waiting=[
                {"gid": "waiting-1", "status": "waiting"},
                {"gid": "waiting-2", "status": "waiting"},
            ],
        )

        self.assertEqual(manager.aria2.force_paused, ["waiting-1", "waiting-2"])
        self.assertNotIn("active-1", manager._serial_gate_paused_gids)
        self.assertEqual(
            manager._visible_aria2_status("paused", "waiting-1"),
            "pending",
        )

    async def test_disk_not_ready_holds_even_active_item(self):
        manager = self.make_manager()
        manager._disk_usage_info = {"free": 0}

        await manager._sync_serial_transfer_gate(
            active=[{"gid": "next-active", "status": "active"}],
            waiting=[],
        )

        self.assertEqual(manager.aria2.force_paused, ["next-active"])
        self.assertEqual(
            manager._visible_aria2_status("active", "next-active"),
            "pending",
        )

    async def test_disabled_serial_mode_releases_only_system_gate_items(self):
        manager = self.make_manager()
        manager.config["upload"]["serial_transfer_mode"] = False
        manager._serial_gate_paused_gids.add("system-held")

        await manager._sync_serial_transfer_gate(
            active=[],
            waiting=[
                {"gid": "system-held", "status": "paused"},
                {"gid": "manual-paused", "status": "paused"},
            ],
        )

        self.assertEqual(manager.aria2.unpaused, ["system-held"])
        self.assertNotIn("system-held", manager._serial_gate_paused_gids)
        self.assertNotIn("manual-paused", manager.aria2.unpaused)

    async def test_resume_in_serial_mode_returns_manual_pause_to_pending_queue(self):
        manager = self.make_manager()
        updates = {}

        original_get_task = task_manager_module.db.get_task
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_task(task_id):
                return {
                    "task_id": task_id,
                    "status": "paused",
                    "aria2_gid": "manual-gid",
                    "download_progress": 0,
                    "upload_progress": 0,
                    "local_path": "",
                }

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_broadcast(*args, **kwargs):
                return None

            task_manager_module.db.get_task = fake_get_task
            task_manager_module.db.update_task = fake_update_task
            manager._broadcast_task_update = fake_broadcast

            result = await manager.resume_task("task-1")
        finally:
            task_manager_module.db.get_task = original_get_task
            task_manager_module.db.update_task = original_update_task

        self.assertTrue(result["success"])
        self.assertEqual(updates["status"], "pending")
        self.assertEqual(manager.aria2.unpaused, [])
        self.assertIn("manual-gid", manager._serial_gate_paused_gids)

    async def test_add_task_uses_pause_option_in_serial_mode(self):
        manager = self.make_manager()
        updates = {}

        original_add_task = task_manager_module.db.add_task
        original_update_task = task_manager_module.db.update_task
        original_get_task = task_manager_module.db.get_task
        try:
            async def fake_add_task(*args, **kwargs):
                return None

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_get_task(task_id):
                return {"task_id": task_id, **updates}

            async def fake_broadcast(*args, **kwargs):
                return None

            task_manager_module.db.add_task = fake_add_task
            task_manager_module.db.update_task = fake_update_task
            task_manager_module.db.get_task = fake_get_task
            manager._broadcast_task_update = fake_broadcast

            await manager.add_task("https://example.test/file.bin", "file.bin")
        finally:
            task_manager_module.db.add_task = original_add_task
            task_manager_module.db.update_task = original_update_task
            task_manager_module.db.get_task = original_get_task

        self.assertEqual(manager.aria2.added[0][1]["pause"], "true")
        self.assertEqual(updates["status"], "pending")
        self.assertIn("new-gid", manager._serial_gate_paused_gids)


if __name__ == "__main__":
    unittest.main()
