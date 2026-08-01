import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

from app.modules.aria2teldrive import task_manager as task_manager_module
from app.modules.tel2teldrive import service as service_module


class FakeTelDrive:
    def __init__(self, parts=None, fetch_error: Exception | None = None):
        self.parts = list(parts or [])
        self.fetch_error = fetch_error
        self.cleaned_upload_ids = []

    def _get_part_message_id(self, part):
        return int(part.get("partId", part.get("id")))

    async def get_upload_parts(self, upload_id):
        if self.fetch_error:
            raise self.fetch_error
        return list(self.parts)

    async def cleanup_upload_session(self, upload_id):
        self.cleaned_upload_ids.append(upload_id)


class UploadPartCleanupSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.manager = task_manager_module.TaskManager()
        self.manager.teldrive = cast(Any, FakeTelDrive())
        self.runtime = SimpleNamespace(
            teldrive_channel_id=-1003854656012,
            telegram_channel_id=-1003854656012,
            telegram_channel_conflict=False,
            db_enabled=True,
        )
        self.active_ids: set[int] | None = set()
        self.delete = AsyncMock(return_value=True)
        self.audit = AsyncMock(return_value={})
        self.get_task = AsyncMock(return_value={"task_id": "task-1", "filename": "movie.mkv"})
        self.update_task = AsyncMock()

        self.original_runtime = service_module.config_store.runtime
        self.original_query = service_module.query_active_teldrive_part_ids
        self.original_delete = service_module.delete_telegram_messages_with_audit
        self.original_audit = service_module.record_telegram_delete_audit
        self.original_get_task = task_manager_module.db.get_task
        self.original_update_task = task_manager_module.db.update_task

        service_module.config_store.runtime = cast(Any, lambda: self.runtime)
        service_module.query_active_teldrive_part_ids = cast(Any, lambda config: self.active_ids)
        service_module.delete_telegram_messages_with_audit = cast(Any, self.delete)
        service_module.record_telegram_delete_audit = cast(Any, self.audit)
        task_manager_module.db.get_task = cast(Any, self.get_task)
        task_manager_module.db.update_task = cast(Any, self.update_task)
        self.manager._record_orphan_parts = cast(Any, AsyncMock())
        self.manager._broadcast_task_update = cast(Any, AsyncMock())

    async def asyncTearDown(self):
        service_module.config_store.runtime = self.original_runtime
        service_module.query_active_teldrive_part_ids = self.original_query
        service_module.delete_telegram_messages_with_audit = self.original_delete
        service_module.record_telegram_delete_audit = self.original_audit
        task_manager_module.db.get_task = self.original_get_task
        task_manager_module.db.update_task = self.original_update_task

    async def test_orphan_cleanup_protects_final_and_active_part_ids(self):
        self.active_ids = {30}

        await self.manager._cleanup_orphan_parts(
            "task-1",
            {
                "upload_id": "upload-1",
                "parts_verified_upload_id": "upload-1",
                "remote_parts": [{"partId": 20}],
                "orphan_parts": [{"partId": 20}, {"partId": 30}, {"partId": 40}],
            },
        )

        self.delete.assert_awaited_once()
        self.assertEqual(self.delete.await_args.args[2], [40])
        self.assertEqual(self.delete.await_args.kwargs["candidate_message_ids"], [20, 30, 40])
        self.assertEqual(self.delete.await_args.kwargs["protected_final_ids"], [20])
        self.assertEqual(self.delete.await_args.kwargs["protected_active_ids"], [30])

    async def test_orphan_cleanup_blocks_when_active_reference_query_fails(self):
        self.active_ids = None

        await self.manager._cleanup_orphan_parts(
            "task-1",
            {
                "upload_id": "upload-1",
                "parts_verified_upload_id": "upload-1",
                "remote_parts": [],
                "orphan_parts": [{"partId": 40}],
            },
        )

        self.delete.assert_not_awaited()
        self.audit.assert_awaited_once()
        self.assertEqual(self.audit.await_args.kwargs["status"], "blocked")
        self.manager._record_orphan_parts.assert_awaited_once()

    async def test_orphan_cleanup_blocks_stale_upload_metadata(self):
        await self.manager._cleanup_orphan_parts(
            "task-1",
            {
                "upload_id": "upload-2",
                "parts_verified_upload_id": "upload-1",
                "remote_parts": [],
                "orphan_parts": [{"partId": 40}],
            },
        )

        self.delete.assert_not_awaited()
        self.assertIn("当前 upload_id", self.audit.await_args.kwargs["detail"])

    async def test_polluted_cleanup_blocks_when_live_fetch_fails(self):
        self.manager.teldrive = cast(Any, FakeTelDrive(fetch_error=RuntimeError("fetch failed")))
        self.manager._upload_session_meta["task-1"] = {
            "upload_id": "upload-1",
            "remote_parts": [{"partId": 61}],
        }
        self.get_task.return_value = {
            "task_id": "task-1",
            "filename": "movie.mkv",
            "error": 'structured_upload_error::{"code":"remote_parts_count_mismatch"}',
        }

        result = await self.manager.cleanup_polluted_upload("task-1")

        self.assertTrue(result["success"])
        self.delete.assert_not_awaited()
        self.assertEqual(self.audit.await_args.kwargs["status"], "blocked")
        self.assertIn("查询失败", self.audit.await_args.kwargs["detail"])
        self.manager._record_orphan_parts.assert_awaited_once()
        self.assertEqual(self.manager.teldrive.cleaned_upload_ids, ["upload-1"])

    async def test_polluted_cleanup_deletes_only_fresh_unreferenced_parts(self):
        self.active_ids = {71}
        self.manager.teldrive = cast(Any, FakeTelDrive(parts=[{"partId": 71}, {"partId": 72}]))
        self.manager._upload_session_meta["task-1"] = {"upload_id": "upload-1"}
        self.get_task.return_value = {
            "task_id": "task-1",
            "filename": "movie.mkv",
            "error": 'structured_upload_error::{"code":"remote_parts_count_mismatch"}',
        }

        result = await self.manager.cleanup_polluted_upload("task-1")

        self.assertTrue(result["success"])
        self.delete.assert_awaited_once()
        self.assertEqual(self.delete.await_args.args[2], [72])
        self.assertEqual(self.delete.await_args.kwargs["candidate_message_ids"], [71, 72])
        self.assertEqual(self.delete.await_args.kwargs["protected_active_ids"], [71])
        self.assertIn("cleaned 1", result["message"])


if __name__ == "__main__":
    unittest.main()
