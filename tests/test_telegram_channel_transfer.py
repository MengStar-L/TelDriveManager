"""Channel imports share relay durability, but never delete source messages.

All database and media writes are confined to a temporary directory. The service
module is replaced with a deletion spy so importing these tests does not create
application configuration or start a Telegram client.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from app import database as db
from app.modules.tel2teldrive import relay


SOURCE_CHANNEL = -1001111111111
TELDRIVE_CHANNEL = -1002222222222


def video_message(message_id=42, document_id=9001, size=18, **overrides):
    values = {
        "id": message_id,
        "document": SimpleNamespace(id=document_id),
        "file": SimpleNamespace(size=size),
        "media": SimpleNamespace(ttl_seconds=None),
        "ttl_period": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class TelegramChannelTransferTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="tdm-channel-transfer-")
        self.root = Path(self.tmp.name).resolve()
        self.original_conn = db._db_conn
        db._db_conn = None
        self.db_path_patch = patch.object(db, "DB_PATH", self.root / "tasks.db")
        self.db_path_patch.start()
        await db.init_db()
        self.payload = b"channel-video-data"
        self.message = video_message(size=len(self.payload))
        self.config = SimpleNamespace(
            relay_enabled=True,
            relay_concurrency=1,
            relay_max_retries=1,
            relay_download_dir=str(self.root / "downloads"),
            relay_download_connections=6,
            telegram_channel_id=TELDRIVE_CHANNEL,
            teldrive_channel_id=TELDRIVE_CHANNEL,
            relay_target_path="/selected-videos",
        )
        self.file_info = {"name": "video.mp4", "size": len(self.payload), "mime_type": "video/mp4"}
        self.client = SimpleNamespace(
            get_messages=AsyncMock(return_value=self.message),
            download_media=AsyncMock(side_effect=self._download),
            delete_messages=AsyncMock(),
            is_connected=Mock(return_value=True),
            is_user_authorized=AsyncMock(return_value=True),
        )
        self.delete_source = AsyncMock(return_value=True)
        self.service_patch = patch.dict(sys.modules, {
            "app.modules.tel2teldrive.service": SimpleNamespace(
                delete_telegram_messages_with_audit=self.delete_source,
            ),
        })
        self.service_patch.start()
        self.budget = SimpleNamespace(reserve=Mock(), release=Mock())
        self.budget_patch = patch.object(relay, "disk_budget", self.budget)
        self.budget_patch.start()
        self.manager = self._manager()

    async def asyncTearDown(self):
        self.budget_patch.stop()
        self.service_patch.stop()
        await db.close_db()
        db._db_conn = self.original_conn
        self.db_path_patch.stop()
        self.tmp.cleanup()

    def _manager(self):
        manager = relay.TelegramRelayManager(Mock(), SimpleNamespace(_broadcast=AsyncMock()))
        manager.config = self.config
        manager.bind_client_getter(lambda: self.client)
        manager._schedule = AsyncMock(return_value=True)
        manager._disk_reserve_bytes = Mock(return_value=1024)
        manager._upload_local_file = AsyncMock(return_value={"success": True, "data": {"id": "td-video"}})
        manager._record_teldrive_mapping = AsyncMock(return_value="td-video")
        return manager

    async def _download(self, message, file, progress_callback=None):
        Path(file).write_bytes(self.payload)
        return str(file)

    async def _enqueue(self, **kwargs):
        return await self.manager.enqueue_channel_video(
            self.config, self.message, self.file_info,
            source_channel_id=SOURCE_CHANNEL, source_title="课程频道", **kwargs,
        )

    async def test_selection_persists_original_channel_and_identity_across_restart(self):
        job = await self._enqueue()
        await db.close_db()
        await db.init_db()
        persisted = await db.get_telegram_relay_job(job["job_id"])

        self.assertEqual(persisted["source_channel_id"], SOURCE_CHANNEL)
        self.assertNotEqual(persisted["source_channel_id"], self.config.teldrive_channel_id)
        self.assertEqual(persisted["source_kind"], "channel")
        self.assertEqual(persisted["source_title"], "课程频道")
        self.assertEqual(persisted["source_document_id"], "9001")
        self.assertEqual(persisted["source_message_id"], self.message.id)

    async def test_repeated_selection_keeps_single_job_and_completed_status(self):
        first = await self._enqueue()
        await db.update_telegram_relay_job(first["job_id"], status="completed", upload_committed=1)
        self.manager._schedule.reset_mock()
        second = await self._enqueue()

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(second["status"], "completed")
        self.assertEqual(len(await db.get_all_telegram_relay_jobs()), 1)
        self.manager._schedule.assert_not_awaited()

    async def test_manual_selection_cannot_adopt_automatic_relay_job(self):
        job_id = relay.make_relay_job_id(SOURCE_CHANNEL, self.message.id)
        await db.add_telegram_relay_job(
            job_id, source_channel_id=SOURCE_CHANNEL, source_message_id=self.message.id,
            file_name="original.mp4", file_size=len(self.payload),
        )
        with self.assertRaisesRegex(ValueError, "自动回源"):
            await self._enqueue()

        persisted = await db.get_telegram_relay_job(job_id)
        self.assertEqual(persisted["source_kind"], "relay")
        self.assertEqual(persisted["file_name"], "original.mp4")
        self.assertEqual(persisted["source_document_id"], "")
        self.manager._schedule.assert_not_awaited()

    async def test_source_kind_cannot_be_changed_by_generic_job_update(self):
        job = await self._enqueue()
        with self.assertRaises(ValueError):
            await db.update_telegram_relay_job(job["job_id"], source_kind="relay")
        self.assertEqual((await db.get_telegram_relay_job(job["job_id"]))["source_kind"], "channel")

    async def test_channel_upload_preserves_source_and_reclaims_local_cache(self):
        job = await self._enqueue()
        await self.manager._process_job(job)
        finished = await db.get_telegram_relay_job(job["job_id"])

        self.client.get_messages.assert_awaited_once_with(SOURCE_CHANNEL, ids=self.message.id)
        self.client.download_media.assert_awaited_once()
        self.manager._upload_local_file.assert_awaited_once()
        self.assertEqual(finished["status"], "completed")
        self.assertTrue(finished["upload_committed"])
        self.assertFalse(finished["source_deleted"])
        self.assertFalse(Path(job["local_path"]).exists())
        self.assertFalse(Path(job["local_path"] + ".part").exists())
        self.delete_source.assert_not_awaited()
        self.client.delete_messages.assert_not_awaited()
        self.budget.reserve.assert_called_once_with(
            "relay:" + job["job_id"], Path(job["local_path"]).parent, len(self.payload), 1024,
        )
        self.budget.release.assert_called_once_with("relay:" + job["job_id"])

    async def test_committed_restart_only_finishes_cleanup_without_source_deletion(self):
        job = await self._enqueue()
        path = Path(job["local_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.payload)
        await db.update_telegram_relay_job(
            job["job_id"], status="cleaning", upload_committed=1,
            upload_result_json=json.dumps({"success": True, "data": {"id": "td-video"}}),
        )
        await db.close_db()
        await db.init_db()
        restarted = self._manager()
        await restarted._process_job(await db.get_telegram_relay_job(job["job_id"]))

        finished = await db.get_telegram_relay_job(job["job_id"])
        self.assertEqual(finished["status"], "completed")
        self.assertFalse(finished["source_deleted"])
        self.assertFalse(path.exists())
        self.client.get_messages.assert_not_awaited()
        self.client.download_media.assert_not_awaited()
        restarted._upload_local_file.assert_not_awaited()
        self.delete_source.assert_not_awaited()
        self.client.delete_messages.assert_not_awaited()
        self.budget.reserve.assert_not_called()

    async def test_large_channel_video_uses_user_session_even_when_bot_pool_is_usable(self):
        self.payload = b"v" * (relay.MULTIBOT_MIN_SIZE + 1)
        self.message.file.size = len(self.payload)
        self.file_info["size"] = len(self.payload)
        pool = SimpleNamespace(usable=Mock(return_value=True), download=AsyncMock(return_value=True))
        self.manager._bot_pool = pool
        job = await self._enqueue()
        await self.manager._process_job(job)

        self.client.download_media.assert_awaited_once()
        pool.download.assert_not_awaited()
        self.delete_source.assert_not_awaited()

    async def test_disk_shortage_leaves_pending_and_never_starts_download(self):
        job = await self._enqueue()
        self.budget.reserve.side_effect = relay.DiskSpaceUnavailable("waiting for disk space")
        await self.manager._run_job(job["job_id"])
        pending = await db.get_telegram_relay_job(job["job_id"])

        self.assertEqual(pending["status"], "pending")
        self.assertIn("waiting for disk space", pending["error"])
        self.assertEqual(pending["retry_count"], 0)
        self.client.download_media.assert_not_awaited()
        self.manager._upload_local_file.assert_not_awaited()
        self.delete_source.assert_not_awaited()
        self.assertFalse(Path(job["local_path"] + ".part").exists())
        self.budget.release.assert_called_once_with("relay:" + job["job_id"])

    async def test_mid_download_disk_pressure_removes_partial_and_keeps_source(self):
        job = await self._enqueue()
        self.budget.reserve.side_effect = [None, relay.DiskSpaceUnavailable("space consumed externally")]

        async def partial_download(message, file, progress_callback=None):
            Path(file).write_bytes(self.payload[:4])
            progress_callback(4, len(self.payload))

        self.client.download_media.side_effect = partial_download
        await self.manager._run_job(job["job_id"])
        pending = await db.get_telegram_relay_job(job["job_id"])

        self.assertEqual(pending["status"], "pending")
        self.assertFalse(Path(job["local_path"] + ".part").exists())
        self.assertFalse(Path(job["local_path"]).exists())
        self.manager._upload_local_file.assert_not_awaited()
        self.delete_source.assert_not_awaited()
        self.budget.release.assert_called_once_with("relay:" + job["job_id"])

    async def test_changed_missing_or_expiring_source_is_rejected_before_download(self):
        job = await self._enqueue()
        variants = [
            None,
            video_message(document_id=9002, size=len(self.payload)),
            video_message(document=None, size=len(self.payload)),
            video_message(ttl_period=30, size=len(self.payload)),
            video_message(media=SimpleNamespace(ttl_seconds=30), size=len(self.payload)),
            video_message(size=len(self.payload) + 1),
        ]
        for changed in variants:
            with self.subTest(source=changed):
                self.client.get_messages.return_value = changed
                with self.assertRaises(RuntimeError):
                    await self.manager._process_job(job)
        self.client.download_media.assert_not_awaited()
        self.manager._upload_local_file.assert_not_awaited()
        self.delete_source.assert_not_awaited()
        self.budget.reserve.assert_not_called()

    async def test_disabled_unknown_size_and_ttl_media_are_not_queued(self):
        self.config.relay_enabled = False
        with self.assertRaises(ValueError):
            await self._enqueue()
        self.config.relay_enabled = True
        self.file_info["size"] = 0
        with self.assertRaises(ValueError):
            await self._enqueue()
        self.file_info["size"] = len(self.payload)
        self.message.ttl_period = 30
        with self.assertRaises(ValueError):
            await self._enqueue()
        self.assertEqual(await db.get_all_telegram_relay_jobs(), [])
        self.manager._schedule.assert_not_awaited()

    async def test_automatic_relay_still_deletes_its_source_after_upload(self):
        job = await self.manager.enqueue_message(self.client, self.config, self.message, self.file_info)
        await self.manager._process_job(job)
        finished = await db.get_telegram_relay_job(job["job_id"])

        self.assertEqual(finished["source_kind"], "relay")
        self.assertEqual(finished["status"], "completed")
        self.assertTrue(finished["source_deleted"])
        self.delete_source.assert_awaited_once()
        self.assertEqual(self.delete_source.await_args.args[2], [self.message.id])
        self.assertEqual(self.delete_source.await_args.kwargs["requested_channel_id"], TELDRIVE_CHANNEL)
        self.assertFalse(Path(job["local_path"]).exists())


if __name__ == "__main__":
    unittest.main()
