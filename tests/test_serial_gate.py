import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast

from app.modules.aria2teldrive import task_manager as task_manager_module


class FakeAria2:
    def __init__(self):
        self.force_paused = []
        self.paused = []
        self.unpaused = []
        self.added = []
        self.removed = []
        self.global_option_changes = []
        self.active = []
        self.waiting = []
        self.stopped = []
        self.status_by_gid = {}

    async def force_pause(self, gid):
        self.force_paused.append(gid)
        return gid

    async def pause(self, gid):
        self.paused.append(gid)
        return gid

    async def unpause(self, gid):
        self.unpaused.append(gid)
        return gid

    async def force_remove(self, gid):
        self.removed.append(gid)
        return gid

    async def remove(self, gid):
        self.removed.append(gid)
        return gid

    async def add_uri(self, url, options=None):
        self.added.append((url, dict(options or {})))
        return f"gid-{len(self.added)}"

    async def change_global_option(self, options):
        self.global_option_changes.append(dict(options or {}))
        return "OK"

    async def tell_active(self):
        return list(self.active)

    async def tell_waiting(self, offset=0, num=1000):
        return list(self.waiting)

    async def tell_stopped_all(self):
        return list(self.stopped)

    async def tell_status(self, gid):
        return self.status_by_gid.get(gid, {"gid": gid, "status": "paused", "files": []})


class SerialGateTests(unittest.IsolatedAsyncioTestCase):
    def make_manager(self, download_dir="."):
        manager = task_manager_module.TaskManager()
        manager.config = {
            "upload": {"serial_transfer_mode": True, "auto_delete": True, "max_retries": 3},
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
        manager._disk_usage_info = {"free": 3 * 1024 ** 3}
        manager.aria2 = cast(Any, FakeAria2())

        async def no_blockers(stopped=None):
            return False

        manager._has_serial_resume_blockers = cast(Any, no_blockers)
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

    async def test_disk_not_ready_does_not_pause_active_item_in_serial_mode(self):
        manager = self.make_manager()
        manager._disk_usage_info = {"free": 0}

        await manager._sync_serial_transfer_gate(
            active=[{"gid": "next-active", "status": "active"}],
            waiting=[],
        )

        self.assertEqual(manager.aria2.force_paused, [])
        self.assertEqual(manager._visible_aria2_status("active", "next-active"), "downloading")

    async def test_dispatch_ignores_low_disk_space_in_serial_mode(self):
        manager = self.make_manager()
        manager._disk_usage_info = {"free": 0}
        queued_task = {
            "task_id": "queued-1",
            "status": "pending",
            "url": "https://example.test/one.bin",
            "filename": "one.bin",
            "aria2_gid": None,
            "aria2_options_json": json.dumps({"dir": ".", "out": "one.bin"}),
        }
        updates = {}

        original_get_next = task_manager_module.db.get_next_pending_queued_task
        original_update_task = task_manager_module.db.update_task
        original_get_all = task_manager_module.db.get_all_tasks
        try:
            async def fake_get_next():
                return queued_task if not updates else None

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_get_all():
                return []

            async def fake_broadcast(*args, **kwargs):
                return None

            cast(Any, task_manager_module.db).get_next_pending_queued_task = fake_get_next
            cast(Any, task_manager_module.db).update_task = fake_update_task
            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
            manager._broadcast_task_update = cast(Any, fake_broadcast)

            released = await manager._dispatch_next_serial_download()
        finally:
            cast(Any, task_manager_module.db).get_next_pending_queued_task = original_get_next
            cast(Any, task_manager_module.db).update_task = original_update_task
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all

        self.assertTrue(released)
        self.assertEqual(len(manager.aria2.added), 1)
        self.assertEqual(updates["status"], "downloading")

    async def test_serial_mode_disables_disk_protection_status_and_limit(self):
        manager = self.make_manager()
        manager._disk_usage_info = {"free": 0}
        manager._disk_protection_active = True
        manager._disk_protection_applied_max_downloads = 3

        await manager._sync_disk_space_download_protection(active_download_count=2)

        self.assertFalse(manager._disk_protection_active)
        self.assertEqual(manager._disk_protection_info["active"], False)
        self.assertEqual(manager._disk_protection_info["message"], "")
        self.assertEqual(
            manager.aria2.global_option_changes[-1]["max-concurrent-downloads"],
            "1",
        )

    async def test_disk_recovered_releases_gate_held_paused_item(self):
        manager = self.make_manager()
        manager._serial_gate_paused_gids.add("held-1")

        await manager._sync_serial_transfer_gate(
            active=[],
            waiting=[{"gid": "held-1", "status": "paused"}],
        )

        self.assertEqual(manager.aria2.unpaused, ["held-1"])
        self.assertNotIn("held-1", manager._serial_gate_paused_gids)

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

    async def test_non_serial_mode_keeps_disk_protection_behavior(self):
        manager = self.make_manager()
        manager.config["upload"]["serial_transfer_mode"] = False
        manager._disk_usage_info = {"free": 0}

        await manager._sync_disk_space_download_protection(active_download_count=2)

        self.assertTrue(manager._disk_protection_active)
        self.assertEqual(manager._disk_protection_info["active"], True)
        self.assertTrue(manager._disk_protection_info["message"])
        self.assertEqual(
            manager.aria2.global_option_changes[-1]["max-concurrent-downloads"],
            "2",
        )

    async def test_add_task_queues_without_touching_aria2_in_serial_mode(self):
        manager = self.make_manager()
        updates = {}
        created = {}

        original_add_task = task_manager_module.db.add_task
        original_update_task = task_manager_module.db.update_task
        original_get_task = task_manager_module.db.get_task
        try:
            async def fake_add_task(task_id, url, filename=None, teldrive_path="/", aria2_options_json=None):
                created.update({
                    "task_id": task_id,
                    "url": url,
                    "filename": filename,
                    "teldrive_path": teldrive_path,
                    "aria2_options_json": aria2_options_json,
                })
                return dict(created)

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_get_task(task_id):
                return {"task_id": task_id, **created, **updates}

            async def fake_broadcast(*args, **kwargs):
                return None

            cast(Any, task_manager_module.db).add_task = fake_add_task
            cast(Any, task_manager_module.db).update_task = fake_update_task
            cast(Any, task_manager_module.db).get_task = fake_get_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)

            task = await manager.add_task("https://example.test/file.bin", "file.bin")
        finally:
            cast(Any, task_manager_module.db).add_task = original_add_task
            cast(Any, task_manager_module.db).update_task = original_update_task
            cast(Any, task_manager_module.db).get_task = original_get_task

        self.assertEqual(manager.aria2.added, [])
        self.assertEqual(task["status"], "pending")
        self.assertIsNone(updates["aria2_gid"])
        saved_options = json.loads(updates["aria2_options_json"])
        self.assertEqual(saved_options["out"], "file.bin")
        self.assertNotIn("pause", saved_options)

    async def test_dispatch_releases_only_one_oldest_db_queued_task(self):
        manager = self.make_manager()
        queued_task = {
            "task_id": "queued-1",
            "status": "pending",
            "url": "https://example.test/one.bin",
            "filename": "one.bin",
            "aria2_gid": None,
            "aria2_options_json": json.dumps({"dir": ".", "out": "one.bin"}),
        }
        updates = {}

        original_get_next = task_manager_module.db.get_next_pending_queued_task
        original_update_task = task_manager_module.db.update_task
        original_get_all = task_manager_module.db.get_all_tasks
        try:
            async def fake_get_next():
                return queued_task if not updates else None

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_get_all():
                return []

            async def fake_broadcast(*args, **kwargs):
                return None

            cast(Any, task_manager_module.db).get_next_pending_queued_task = fake_get_next
            cast(Any, task_manager_module.db).update_task = fake_update_task
            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
            manager._broadcast_task_update = cast(Any, fake_broadcast)

            released = await manager._dispatch_next_serial_download()
        finally:
            cast(Any, task_manager_module.db).get_next_pending_queued_task = original_get_next
            cast(Any, task_manager_module.db).update_task = original_update_task
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all

        self.assertTrue(released)
        self.assertEqual(len(manager.aria2.added), 1)
        self.assertEqual(manager.aria2.added[0][0], queued_task["url"])
        self.assertEqual(updates["status"], "downloading")
        self.assertEqual(updates["aria2_gid"], "gid-1")

    async def test_dispatch_stays_pending_when_cleanup_blocker_exists(self):
        manager = self.make_manager()

        async def has_blocker(stopped=None):
            return True

        manager._has_serial_resume_blockers = cast(Any, has_blocker)

        original_get_next = task_manager_module.db.get_next_pending_queued_task
        original_get_all = task_manager_module.db.get_all_tasks
        try:
            async def fake_get_next():
                return {
                    "task_id": "queued-1",
                    "status": "pending",
                    "url": "https://example.test/one.bin",
                    "filename": "one.bin",
                    "aria2_gid": None,
                    "aria2_options_json": "{}",
                }

            async def fake_get_all():
                return []

            cast(Any, task_manager_module.db).get_next_pending_queued_task = fake_get_next
            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
            released = await manager._dispatch_next_serial_download()
        finally:
            cast(Any, task_manager_module.db).get_next_pending_queued_task = original_get_next
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all

        self.assertFalse(released)
        self.assertEqual(manager.aria2.added, [])

    async def test_dispatch_stays_pending_when_db_download_in_flight(self):
        manager = self.make_manager()

        original_get_next = task_manager_module.db.get_next_pending_queued_task
        original_get_all = task_manager_module.db.get_all_tasks
        try:
            async def fake_get_next():
                return {
                    "task_id": "queued-2",
                    "status": "pending",
                    "url": "https://example.test/two.bin",
                    "filename": "two.bin",
                    "aria2_gid": None,
                    "aria2_options_json": "{}",
                }

            async def fake_get_all():
                return [{"task_id": "downloading-1", "status": "downloading", "aria2_gid": "gid-current"}]

            cast(Any, task_manager_module.db).get_next_pending_queued_task = fake_get_next
            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all

            released = await manager._dispatch_next_serial_download()
        finally:
            cast(Any, task_manager_module.db).get_next_pending_queued_task = original_get_next
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all

        self.assertFalse(released)
        self.assertEqual(manager.aria2.added, [])

    async def test_dispatch_stays_pending_when_live_pending_gid_holds_slot(self):
        manager = self.make_manager()
        manager.aria2.status_by_gid["held-gid"] = {
            "gid": "held-gid",
            "status": "paused",
            "files": [],
        }

        original_get_next = task_manager_module.db.get_next_pending_queued_task
        original_get_all = task_manager_module.db.get_all_tasks
        try:
            async def fake_get_next():
                return {
                    "task_id": "queued-2",
                    "status": "pending",
                    "url": "https://example.test/two.bin",
                    "filename": "two.bin",
                    "aria2_gid": None,
                    "aria2_options_json": "{}",
                }

            async def fake_get_all():
                return [
                    {"task_id": "held-task", "status": "pending", "aria2_gid": "held-gid"},
                    {"task_id": "queued-2", "status": "pending", "aria2_gid": None},
                ]

            cast(Any, task_manager_module.db).get_next_pending_queued_task = fake_get_next
            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all

            released = await manager._dispatch_next_serial_download()
        finally:
            cast(Any, task_manager_module.db).get_next_pending_queued_task = original_get_next
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all

        self.assertFalse(released)
        self.assertEqual(manager.aria2.added, [])

    async def test_normalize_pending_stale_gid_removes_aria2_and_local_residue(self):
        with tempfile.TemporaryDirectory() as tmp:
            residue = Path(tmp) / "queued.bin"
            residue.write_bytes(b"partial")
            aria2_control = Path(f"{residue}.aria2")
            aria2_control.write_bytes(b"control")
            manager = self.make_manager(download_dir=tmp)
            task = {
                "task_id": "queued-1",
                "status": "pending",
                "url": "https://example.test/queued.bin",
                "filename": "queued.bin",
                "aria2_gid": "old-gid",
                "local_path": str(residue),
                "aria2_options_json": json.dumps({"dir": tmp, "out": "queued.bin"}),
            }
            manager.aria2.status_by_gid["old-gid"] = {
                "gid": "old-gid",
                "status": "removed",
                "dir": tmp,
                "files": [{"path": str(residue), "uris": []}],
            }
            updates = {}

            original_get_all = task_manager_module.db.get_all_tasks
            original_update_task = task_manager_module.db.update_task
            try:
                async def fake_get_all():
                    return [task]

                async def fake_update_task(task_id, **kwargs):
                    updates.update(kwargs)

                async def fake_broadcast(*args, **kwargs):
                    return None

                async def fake_check_disk():
                    manager._disk_usage_info = {"free": 3 * 1024 ** 3}

                cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
                cast(Any, task_manager_module.db).update_task = fake_update_task
                manager._broadcast_task_update = cast(Any, fake_broadcast)
                manager._check_disk_usage = cast(Any, fake_check_disk)

                removed = await manager._normalize_serial_pending_aria2_tasks()
            finally:
                cast(Any, task_manager_module.db).get_all_tasks = original_get_all
                cast(Any, task_manager_module.db).update_task = original_update_task

            self.assertEqual(removed, {"old-gid"})
            self.assertEqual(manager.aria2.removed, ["old-gid"])
            self.assertFalse(residue.exists())
            self.assertFalse(aria2_control.exists())
            self.assertIsNone(updates["aria2_gid"])
            self.assertEqual(updates["status"], "pending")

    async def test_normalize_keeps_live_pending_gid_and_residue(self):
        with tempfile.TemporaryDirectory() as tmp:
            residue = Path(tmp) / "queued.bin"
            residue.write_bytes(b"partial")
            aria2_control = Path(f"{residue}.aria2")
            aria2_control.write_bytes(b"control")
            manager = self.make_manager(download_dir=tmp)
            task = {
                "task_id": "queued-live",
                "status": "pending",
                "url": "https://example.test/queued.bin",
                "filename": "queued.bin",
                "aria2_gid": "live-gid",
                "local_path": str(residue),
                "aria2_options_json": json.dumps({"dir": tmp, "out": "queued.bin"}),
            }
            manager.aria2.status_by_gid["live-gid"] = {
                "gid": "live-gid",
                "status": "paused",
                "dir": tmp,
                "files": [{"path": str(residue), "uris": []}],
            }

            original_get_all = task_manager_module.db.get_all_tasks
            original_update_task = task_manager_module.db.update_task
            try:
                async def fake_get_all():
                    return [task]

                async def fake_update_task(task_id, **kwargs):
                    raise AssertionError(f"should not update live pending task: {task_id} {kwargs}")

                async def fake_broadcast(*args, **kwargs):
                    return None

                cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
                cast(Any, task_manager_module.db).update_task = fake_update_task
                manager._broadcast_task_update = cast(Any, fake_broadcast)

                removed = await manager._normalize_serial_pending_aria2_tasks()
            finally:
                cast(Any, task_manager_module.db).get_all_tasks = original_get_all
                cast(Any, task_manager_module.db).update_task = original_update_task

            self.assertEqual(removed, set())
            self.assertEqual(manager.aria2.removed, [])
            self.assertTrue(residue.exists())
            self.assertTrue(aria2_control.exists())
            self.assertIn("live-gid", manager._serial_gate_paused_gids)

    async def test_resume_no_gid_manual_pause_returns_to_pending_queue(self):
        manager = self.make_manager()
        updates = {}

        original_get_task = task_manager_module.db.get_task
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_task(task_id):
                return {
                    "task_id": task_id,
                    "status": "paused",
                    "aria2_gid": None,
                    "download_progress": 0,
                    "upload_progress": 0,
                    "local_path": "",
                }

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_broadcast(*args, **kwargs):
                return None

            cast(Any, task_manager_module.db).get_task = fake_get_task
            cast(Any, task_manager_module.db).update_task = fake_update_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)

            async def fake_dispatch(*args, **kwargs):
                return False

            manager._dispatch_next_serial_download = cast(Any, fake_dispatch)

            result = await manager.resume_task("task-1")
        finally:
            cast(Any, task_manager_module.db).get_task = original_get_task
            cast(Any, task_manager_module.db).update_task = original_update_task

        self.assertTrue(result["success"])
        self.assertEqual(updates["status"], "pending")
        self.assertEqual(manager.aria2.unpaused, [])
        self.assertEqual(manager.aria2.added, [])

    async def test_auto_retry_counts_consecutive_failures_without_progress(self):
        manager = self.make_manager()
        manager.config["upload"]["max_retries"] = 3
        manager._upload_retry_counts["task-1"] = 1
        manager._upload_retry_checkpoints["task-1"] = 10
        manager._upload_confirmed_checkpoints["task-1"] = 10
        manager._set_runtime_task_fields("task-1", upload_chunk_done=10, upload_chunk_total=26)
        created_tasks = []
        updates = []

        original_get_all = task_manager_module.db.get_all_tasks
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_all():
                return [{
                    "task_id": "task-1",
                    "status": "failed",
                    "download_progress": 100.0,
                    "upload_progress": 0.0,
                    "local_path": "resume.bin",
                    "error": "timeout",
                }]

            async def fake_update_task(task_id, **kwargs):
                updates.append(kwargs)
                return None

            async def fake_broadcast(*args, **kwargs):
                return None

            async def fake_retry_upload(task_id):
                created_tasks.append(task_id)
                return None

            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
            cast(Any, task_manager_module.db).update_task = fake_update_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)
            manager._retry_upload = cast(Any, fake_retry_upload)
            manager._get_upload_path = cast(Any, lambda path: path)
            manager._count_path_chunks = cast(Any, lambda path: 26)

            original_exists = task_manager_module.os.path.exists
            task_manager_module.os.path.exists = lambda path: True
            try:
                await manager._auto_retry_failed_uploads()
                await asyncio.sleep(0)
            finally:
                task_manager_module.os.path.exists = original_exists
        finally:
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all
            cast(Any, task_manager_module.db).update_task = original_update_task

        self.assertEqual(manager._upload_retry_counts["task-1"], 2)
        self.assertEqual(manager._upload_retry_checkpoints["task-1"], 10)
        self.assertIn("task-1", created_tasks)
        self.assertAlmostEqual(updates[-1]["upload_progress"], 38.5)
        self.assertEqual(
            manager._runtime_task_state["task-1"]["upload_chunk_done"],
            10,
        )

    async def test_auto_retry_resets_budget_after_chunk_progress(self):
        manager = self.make_manager()
        manager.config["upload"]["max_retries"] = 3
        manager._upload_retry_counts["task-2"] = 3
        manager._upload_retry_checkpoints["task-2"] = 10
        manager._upload_confirmed_checkpoints["task-2"] = 10
        manager._set_runtime_task_fields("task-2", upload_chunk_done=14, upload_chunk_total=26)
        created_tasks = []
        updates = []

        original_get_all = task_manager_module.db.get_all_tasks
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_all():
                return [{
                    "task_id": "task-2",
                    "status": "failed",
                    "download_progress": 100.0,
                    "upload_progress": 0.0,
                    "local_path": "resume.bin",
                    "error": "timeout",
                }]

            async def fake_update_task(task_id, **kwargs):
                updates.append(kwargs)
                return None

            async def fake_broadcast(*args, **kwargs):
                return None

            async def fake_retry_upload(task_id):
                created_tasks.append(task_id)
                return None

            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
            cast(Any, task_manager_module.db).update_task = fake_update_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)
            manager._retry_upload = cast(Any, fake_retry_upload)
            manager._get_upload_path = cast(Any, lambda path: path)
            manager._count_path_chunks = cast(Any, lambda path: 26)

            original_exists = task_manager_module.os.path.exists
            task_manager_module.os.path.exists = lambda path: True
            try:
                await manager._auto_retry_failed_uploads()
                await asyncio.sleep(0)
            finally:
                task_manager_module.os.path.exists = original_exists
        finally:
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all
            cast(Any, task_manager_module.db).update_task = original_update_task

        self.assertEqual(manager._upload_retry_counts["task-2"], 1)
        self.assertEqual(manager._upload_retry_checkpoints["task-2"], 14)
        self.assertEqual(manager._upload_confirmed_checkpoints["task-2"], 14)
        self.assertIn("task-2", created_tasks)
        self.assertAlmostEqual(updates[-1]["upload_progress"], 53.8)
        self.assertEqual(
            manager._runtime_task_state["task-2"]["upload_chunk_done"],
            14,
        )

    async def test_resume_uploading_preserves_confirmed_chunk_baseline(self):
        manager = self.make_manager()
        manager._upload_retry_counts["task-4"] = 2
        manager._upload_retry_checkpoints["task-4"] = 8
        manager._upload_confirmed_checkpoints["task-4"] = 14
        updates = {}
        created_tasks = []

        original_get_task = task_manager_module.db.get_task
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_task(task_id):
                return {
                    "task_id": task_id,
                    "status": "paused",
                    "download_progress": 100.0,
                    "upload_progress": 0.0,
                    "local_path": "resume.bin",
                }

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_broadcast(*args, **kwargs):
                return None

            async def fake_retry_upload(task_id):
                created_tasks.append(task_id)
                return None

            cast(Any, task_manager_module.db).get_task = fake_get_task
            cast(Any, task_manager_module.db).update_task = fake_update_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)
            manager._retry_upload = cast(Any, fake_retry_upload)
            manager._get_upload_path = cast(Any, lambda path: path)
            manager._count_path_chunks = cast(Any, lambda path: 26)

            original_exists = task_manager_module.os.path.exists
            task_manager_module.os.path.exists = lambda path: True
            try:
                result = await manager.resume_task("task-4")
                await asyncio.sleep(0)
            finally:
                task_manager_module.os.path.exists = original_exists
        finally:
            cast(Any, task_manager_module.db).get_task = original_get_task
            cast(Any, task_manager_module.db).update_task = original_update_task

        self.assertTrue(result["success"])
        self.assertAlmostEqual(updates["upload_progress"], 53.8)
        self.assertEqual(
            manager._runtime_task_state["task-4"]["upload_chunk_done"],
            14,
        )
        self.assertNotIn("task-4", manager._upload_retry_counts)
        self.assertNotIn("task-4", manager._upload_retry_checkpoints)
        self.assertEqual(manager._upload_confirmed_checkpoints["task-4"], 14)
        self.assertIn("task-4", created_tasks)

    async def test_retry_uploading_preserves_confirmed_chunk_baseline(self):
        manager = self.make_manager()
        manager._upload_retry_counts["task-5"] = 2
        manager._upload_retry_checkpoints["task-5"] = 8
        manager._upload_confirmed_checkpoints["task-5"] = 14
        updates = {}
        created_tasks = []

        original_get_task = task_manager_module.db.get_task
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_task(task_id):
                return {
                    "task_id": task_id,
                    "status": "failed",
                    "download_progress": 100.0,
                    "upload_progress": 0.0,
                    "aria2_gid": "gid-5",
                    "local_path": "resume.bin",
                }

            async def fake_update_task(task_id, **kwargs):
                updates.update(kwargs)

            async def fake_broadcast(*args, **kwargs):
                return None

            async def fake_retry_upload(task_id):
                created_tasks.append(task_id)
                return None

            cast(Any, task_manager_module.db).get_task = fake_get_task
            cast(Any, task_manager_module.db).update_task = fake_update_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)
            manager._retry_upload = cast(Any, fake_retry_upload)
            manager._get_upload_path = cast(Any, lambda path: path)
            manager._count_path_chunks = cast(Any, lambda path: 26)

            original_exists = task_manager_module.os.path.exists
            task_manager_module.os.path.exists = lambda path: True
            try:
                result = await manager.retry_task("task-5")
                await asyncio.sleep(0)
            finally:
                task_manager_module.os.path.exists = original_exists
        finally:
            cast(Any, task_manager_module.db).get_task = original_get_task
            cast(Any, task_manager_module.db).update_task = original_update_task

        self.assertTrue(result["success"])
        self.assertAlmostEqual(updates["upload_progress"], 53.8)
        self.assertEqual(
            manager._runtime_task_state["task-5"]["upload_chunk_done"],
            14,
        )
        self.assertNotIn("task-5", manager._upload_retry_counts)
        self.assertNotIn("task-5", manager._upload_retry_checkpoints)
        self.assertEqual(manager._upload_confirmed_checkpoints["task-5"], 14)
        self.assertIn("task-5", created_tasks)

    async def test_pause_uploading_clears_retry_state(self):
        manager = self.make_manager()
        manager._upload_retry_counts["task-3"] = 2
        manager._upload_retry_checkpoints["task-3"] = 8
        manager._upload_confirmed_checkpoints["task-3"] = 8

        original_get_task = task_manager_module.db.get_task
        original_update_task = task_manager_module.db.update_task
        try:
            async def fake_get_task(task_id):
                return {
                    "task_id": task_id,
                    "status": "uploading",
                    "aria2_gid": "gid-3",
                    "local_path": "resume.bin",
                }

            async def fake_update_task(task_id, **kwargs):
                return None

            async def fake_broadcast(*args, **kwargs):
                return None

            task_manager_module.db.get_task = fake_get_task
            task_manager_module.db.update_task = fake_update_task
            manager._broadcast_task_update = cast(Any, fake_broadcast)

            result = await manager.pause_task("task-3")
        finally:
            task_manager_module.db.get_task = original_get_task
            task_manager_module.db.update_task = original_update_task

        self.assertTrue(result["success"])
        self.assertNotIn("task-3", manager._upload_retry_counts)
        self.assertNotIn("task-3", manager._upload_retry_checkpoints)
        self.assertEqual(manager._upload_confirmed_checkpoints["task-3"], 8)


if __name__ == "__main__":
    unittest.main()
