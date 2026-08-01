import unittest
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

from app.modules.tel2teldrive import service
from app.modules.tel2teldrive import routes


class FakeBroker:
    def __init__(self):
        self.events = []

    def _schedule_broadcast(self, event):
        self.events.append(event)


def make_runtime(**overrides):
    values = {
        "teldrive_channel_id": -1003854656012,
        "telegram_channel_id": -1003854656012,
        "telegram_channel_conflict": False,
        "log_buffer_size": 100,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class TelegramDeleteAuditTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_add_progress_log = service.db.add_progress_log
        self.original_broker = service.broker
        self.add_progress_log = AsyncMock(return_value={"id": 7})
        self.broker = FakeBroker()
        service.db.add_progress_log = cast(Any, self.add_progress_log)
        service.broker = cast(Any, self.broker)

    async def asyncTearDown(self):
        service.db.add_progress_log = self.original_add_progress_log
        service.broker = self.original_broker

    async def test_success_persists_one_final_record(self):
        client = SimpleNamespace(is_connected=lambda: True, delete_messages=AsyncMock())

        result = await service.delete_telegram_messages_with_audit(
            client,
            make_runtime(),
            [12, 11, 12],
            reason="duplicate_incoming_message",
            file_names=["movie.mkv"],
        )

        self.assertTrue(result)
        client.delete_messages.assert_awaited_once_with(-1003854656012, [12, 11])
        self.assertEqual(self.add_progress_log.await_count, 1)
        payload = self.add_progress_log.await_args.args[1]
        self.assertEqual(payload["status"], "deleted")
        self.assertEqual(payload["message_ids"], [12, 11])
        self.assertEqual(payload["deleted_message_ids"], [12, 11])
        self.assertIsNotNone(datetime.fromisoformat(payload["occurred_at"]).tzinfo)
        self.assertEqual(self.broker.events[0]["type"], "telegram_delete_audit")

    async def test_failure_persists_one_final_record(self):
        client = SimpleNamespace(
            is_connected=lambda: True,
            delete_messages=AsyncMock(side_effect=RuntimeError("telegram unavailable")),
        )

        result = await service.delete_telegram_messages_with_audit(
            client,
            make_runtime(),
            [21],
            reason="relay_source_after_upload",
            job_id="job-1",
        )

        self.assertFalse(result)
        self.assertEqual(self.add_progress_log.await_count, 1)
        payload = self.add_progress_log.await_args.args[1]
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["job_id"], "job-1")
        self.assertIn("telegram unavailable", payload["detail"])

    async def test_conflicting_config_blocks_without_calling_telegram(self):
        client = SimpleNamespace(is_connected=lambda: True, delete_messages=AsyncMock())

        result = await service.delete_telegram_messages_with_audit(
            client,
            make_runtime(telegram_channel_conflict=True),
            [31],
            reason="polluted_upload_parts",
            task_id="task-1",
            upload_id="upload-1",
        )

        self.assertFalse(result)
        client.delete_messages.assert_not_awaited()
        payload = self.add_progress_log.await_args.args[1]
        self.assertEqual(payload["status"], "blocked")
        self.assertIn("冲突", payload["detail"])

    async def test_mismatched_requested_channel_is_blocked(self):
        client = SimpleNamespace(is_connected=lambda: True, delete_messages=AsyncMock())

        result = await service.delete_telegram_messages_with_audit(
            client,
            make_runtime(),
            [41],
            reason="relay_source_after_upload",
            requested_channel_id=-1003819048300,
        )

        self.assertFalse(result)
        client.delete_messages.assert_not_awaited()
        self.assertEqual(self.add_progress_log.await_args.args[1]["status"], "blocked")

    async def test_explicit_blocked_decision_records_without_deletion(self):
        await service.record_telegram_delete_audit(
            status="blocked",
            reason="upload_orphan_parts",
            channel_id=-1003854656012,
            message_ids=[51],
            protected_final_ids=[51],
            detail="候选消息仍被最终文件引用",
        )

        payload = self.add_progress_log.await_args.args[1]
        self.assertEqual(payload["protected_final_ids"], [51])
        self.assertEqual(payload["deleted_message_ids"], [])


class TelegramDeleteAuditRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_query_returns_newest_first_structured_items(self):
        original = service.db.get_progress_logs
        service.db.get_progress_logs = cast(
            Any,
            AsyncMock(
                return_value=[
                    {
                        "id": 1,
                        "job_id": "task-1",
                        "created_at": "2026-07-31T10:00:00+08:00",
                        "payload": {"status": "blocked", "occurred_at": "2026-07-31T10:00:00+08:00"},
                    },
                    {
                        "id": 2,
                        "job_id": "task-2",
                        "created_at": "2026-07-31T11:00:00+08:00",
                        "payload": {"status": "deleted", "occurred_at": "2026-07-31T11:00:00+08:00"},
                    },
                ]
            ),
        )
        try:
            result = await routes.get_telegram_delete_logs(limit=50)
        finally:
            service.db.get_progress_logs = original

        self.assertEqual([item["id"] for item in result["items"]], [2, 1])
        self.assertEqual(result["items"][0]["status"], "deleted")

    async def test_clear_isolated_telegram_audit_stream(self):
        original = service.db.clear_progress_logs
        clear = AsyncMock(return_value=3)
        service.db.clear_progress_logs = cast(Any, clear)
        try:
            result = await routes.clear_telegram_delete_logs()
        finally:
            service.db.clear_progress_logs = original

        self.assertEqual(result, {"success": True, "count": 3})
        clear.assert_awaited_once_with(stream=service.TELEGRAM_DELETE_LOG_STREAM)


if __name__ == "__main__":
    unittest.main()
