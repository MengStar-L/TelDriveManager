"""Channel imports share relay durability, but never delete source messages.

All database and media writes are confined to a temporary directory. The service
module is replaced with a deletion spy so importing these tests does not create
application configuration or start a Telegram client.
"""

import asyncio
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
            teldrive_target_path="/default-videos",
            teldrive_url="https://teldrive.invalid",
            bearer_token="test-only-token",
            teldrive_chunk_size="100M",
            teldrive_upload_concurrency=1,
            teldrive_random_chunk_name=False,
            upload_max_retries=1,
            upload_min_throughput_kbps=100,
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

    async def test_channel_destination_is_normalized_and_persisted_before_scheduling(self):
        scheduled_targets = []

        async def observe_schedule(job_id):
            scheduled_targets.append((await db.get_telegram_relay_job(job_id))["target_path"])
            return True

        self.manager._schedule.side_effect = observe_schedule
        job = await self._enqueue(target_path=" /课程//2026/ ")
        await db.close_db()
        await db.init_db()
        persisted = await db.get_telegram_relay_job(job["job_id"])

        self.assertEqual(persisted["target_path"], "/课程/2026")
        self.assertEqual(scheduled_targets, ["/课程/2026"])

    async def test_channel_default_destination_is_snapshotted_at_enqueue(self):
        for message_id, explicit, relay_path, default_path, expected in (
            (101, None, "/relay-first", "/default-first", "/relay-first"),
            (102, "", "", "/default-first", "/default-first"),
            (103, None, "", "", "/"),
            (104, "/", "/relay-first", "/default-first", "/"),
        ):
            with self.subTest(message_id=message_id):
                self.message.id = message_id
                self.config.relay_target_path = relay_path
                self.config.teldrive_target_path = default_path
                job = await self._enqueue(target_path=explicit)
                self.config.relay_target_path = "/changed-later"
                self.config.teldrive_target_path = "/another-default"
                stored = await db.get_telegram_relay_job(job["job_id"])
                self.assertEqual(stored["target_path"], expected)

    async def test_invalid_channel_destination_is_rejected_without_queueing(self):
        for target in ("/videos/../other", "C:\\downloads", "https://teldrive.invalid/videos", "/bad\x00name"):
            with self.subTest(target=target):
                with self.assertRaises(ValueError):
                    await self._enqueue(target_path=target)
        self.assertEqual(await db.get_all_telegram_relay_jobs(), [])
        self.manager._schedule.assert_not_awaited()

    async def test_manual_and_automatic_selection_share_identity_without_retargeting(self):
        # Both entry points use enqueue_channel_video; competing selections must
        # keep the first committed source metadata and destination.
        manual = await self._enqueue(target_path="/manual-destination")
        await db.update_telegram_relay_job(manual["job_id"], status="failed", error="temporary upload failure")
        original_file_name = self.file_info["name"]
        self.file_info["name"] = "new-name.mp4"
        self.message.document.id = 9999
        automatic, duplicate = await asyncio.gather(
            self._enqueue(target_path="/automatic-destination"),
            self._enqueue(target_path="/changed-channel-destination"),
        )
        self.assertEqual({manual["job_id"], automatic["job_id"], duplicate["job_id"]}, {manual["job_id"]})
        self.assertEqual(len(await db.get_all_telegram_relay_jobs()), 1)
        stored = await db.get_telegram_relay_job(manual["job_id"])
        self.assertEqual(stored["target_path"], "/manual-destination")
        self.assertEqual(stored["source_document_id"], "9001")
        self.assertEqual(stored["file_name"], original_file_name)
        self.assertEqual(stored["status"], "failed")
        self.assertEqual(stored["error"], "temporary upload failure")

    async def test_channel_upload_retry_keeps_destination_and_confirmed_parts_after_restart(self):
        job = await self._enqueue(target_path="/channel-original")
        path = Path(job["local_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.payload)
        calls = []
        cleanup = AsyncMock()
        confirmed_part = {"partNo": 1, "partId": 7001, "size": len(self.payload)}

        class FakeTelDrive:
            def __init__(self, **kwargs):
                self.chunk_size = 100 * 1024 * 1024

            cleanup_upload_session = cleanup

            async def upload_file_chunked(self, file, target, progress, **kwargs):
                calls.append({"target": target, "upload_id": kwargs["upload_id"],
                              "confirmed": list(kwargs["confirmed_part_numbers"]),
                              "parts": list(kwargs["remote_parts"])})
                if len(calls) == 1:
                    await kwargs["part_confirm_callback"](1, confirmed_part, [1], [confirmed_part], 1)
                    raise RuntimeError("finalize response interrupted")
                return {"success": True, "data": {"id": "td-resumed-video"}}

        with patch.object(relay, "TelDriveClient", FakeTelDrive):
            self.config.relay_target_path = "/changed-before-first-upload"
            with self.assertRaisesRegex(RuntimeError, "finalize response interrupted"):
                await relay.TelegramRelayManager._upload_local_file(self.manager, path, self.config, job)
            first_checkpoint = await db.get_telegram_relay_job(job["job_id"])
            await db.update_telegram_relay_job(job["job_id"], status="failed")
            await db.close_db()
            await db.init_db()
            self.config.relay_target_path = "/changed-after-restart"
            self.config.teldrive_target_path = "/new-global-default"
            restarted = self._manager()
            retried = await restarted.retry_job(job["job_id"])
            self.assertTrue(retried["success"])
            self.assertEqual(retried["data"]["target_path"], "/channel-original")
            result = await relay.TelegramRelayManager._upload_local_file(
                restarted, path, self.config, retried["data"])

        self.assertTrue(result["success"])
        self.assertEqual([item["target"] for item in calls], ["/channel-original", "/channel-original"])
        self.assertEqual(calls[1]["upload_id"], first_checkpoint["upload_id"])
        self.assertEqual(calls[1]["confirmed"], [1])
        self.assertEqual(calls[1]["parts"], [confirmed_part])
        cleanup.assert_not_awaited()
        self.delete_source.assert_not_awaited()

    async def test_ordinary_relay_still_resolves_default_when_upload_begins(self):
        job = await self.manager.enqueue_message(self.client, self.config, self.message, self.file_info)
        self.assertEqual(job["target_path"], "")
        path = Path(job["local_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.payload)
        self.config.relay_target_path = "/latest-relay-setting"
        upload = AsyncMock(return_value={"success": True, "data": {"id": "td-ordinary"}})
        fake_client = SimpleNamespace(chunk_size=100 * 1024 * 1024, upload_file_chunked=upload)
        with patch.object(relay, "TelDriveClient", return_value=fake_client):
            await relay.TelegramRelayManager._upload_local_file(self.manager, path, self.config, job)

        self.assertEqual(upload.await_args.args[1], "/latest-relay-setting")
        self.assertEqual((await db.get_telegram_relay_job(job["job_id"]))["target_path"], "/latest-relay-setting")

    async def test_automatic_receipt_survives_post_insert_failure_and_job_deletion(self):
        from datetime import datetime, timezone
        from telethon.tl.types import Channel, ChatPhotoEmpty
        from app.modules.tel2teldrive import catalog as catalog_module

        now = datetime.now(timezone.utc)
        entity = Channel(id=1111111111, title="课程频道", photo=ChatPhotoEmpty(), date=now,
                         broadcast=True, access_hash=9, username="videos", noforwards=True)
        self.message.date = now
        self.message.document.size = len(self.payload)
        self.message.document.mime_type = "video/mp4"
        self.message.document.attributes = []
        self.client.get_entity = AsyncMock(return_value=entity)
        self.client.get_messages.return_value = [self.message]

        def make_catalog():
            return catalog_module.TelegramVideoCatalog(
                lambda: self.client, lambda: self.config, self.manager, Mock(), None)

        catalog = make_catalog()
        try:
            await catalog.add_channel("@videos")
            await catalog._write([catalog._video_statement(
                SOURCE_CHANNEL, entity, self.message, catalog_module.video_file_info(self.message))])
            await catalog.update_channel(SOURCE_CHANNEL, auto_sync=True, target_path="/channel-original",
                                         sync_since="2020-01-01T00:00:00Z")
            # The production relay commits its INSERT before notifying browsers.
            # Losing that notification must never undo the atomic dispatch receipt.
            self.manager.broker._broadcast.side_effect = RuntimeError("broadcast failed after INSERT")
            self.assertEqual(await catalog._dispatch_auto(SOURCE_CHANNEL), 1)
            jobs = await db.get_all_telegram_relay_jobs()
            self.assertEqual(len(jobs), 1)
            receipt = (await catalog._rows("SELECT * FROM telegram_catalog_dispatches"))[0]
            self.assertEqual(receipt["state"], "submitted")
            self.assertEqual(receipt["job_id"], jobs[0]["job_id"])
            self.assertEqual(receipt["target_path"], "/channel-original")
            self.assertEqual(receipt["attempts"], 0)

            await db.update_telegram_relay_job(jobs[0]["job_id"], status="completed")
            await db.delete_telegram_relay_job(jobs[0]["job_id"])
            await catalog.stop()
            catalog = make_catalog()
            await catalog.update_channel(SOURCE_CHANNEL, target_path="/changed-channel-directory")
            self.client.get_messages.reset_mock()
            self.assertEqual(await catalog._dispatch_auto(SOURCE_CHANNEL), 0)
            self.assertEqual(await db.get_all_telegram_relay_jobs(), [])
            persisted = (await catalog._rows("SELECT * FROM telegram_catalog_dispatches"))[0]
            self.assertEqual(persisted["state"], "submitted")
            self.assertEqual(persisted["job_status"], "completed")
            self.assertEqual(persisted["target_path"], "/channel-original")
            self.client.get_messages.assert_not_awaited()
            self.delete_source.assert_not_awaited()
        finally:
            await catalog.stop()

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
