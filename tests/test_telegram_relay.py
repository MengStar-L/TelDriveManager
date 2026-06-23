import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from app import database as db
from app.modules.tel2teldrive import service as service_module
from app.modules.tel2teldrive.relay import TelegramRelayManager


class FakeLogger:
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
        self.events = []

    async def _broadcast(self, event):
        self.events.append(dict(event))


class FakeTelegramClient:
    def __init__(self, payload: bytes = b"telegram-file"):
        self.payload = payload
        self.deleted_messages = []
        self.download_calls = []

    async def get_messages(self, channel_id, ids):
        return SimpleNamespace(id=ids, channel_id=channel_id)

    async def download_media(self, message, file, progress_callback=None):
        path = Path(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.payload)
        self.download_calls.append((message.id, str(path)))
        if progress_callback:
            progress_callback(len(self.payload), len(self.payload))
        await asyncio.sleep(0)
        return str(path)

    async def delete_messages(self, channel_id, message_ids):
        self.deleted_messages.append((channel_id, list(message_ids)))
        return True


class TelegramRelayDatabaseMixin:
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.original_db_path = db.DB_PATH
        await db.close_db()
        db.DB_PATH = Path(self.tmp.name) / "tasks.db"
        await db.init_db()
        service_module._ignored_deleted_message_ids.clear()

    async def asyncTearDown(self):
        await db.close_db()
        db.DB_PATH = self.original_db_path
        self.tmp.cleanup()
        service_module._ignored_deleted_message_ids.clear()

    def make_config(self, **overrides):
        base = {
            "telegram_channel_id": 12345,
            "relay_enabled": True,
            "relay_concurrency": 1,
            "relay_max_retries": 1,
            "relay_download_dir": str(Path(self.tmp.name) / "relay"),
            "teldrive_url": "http://teldrive.local",
            "bearer_token": "token",
            "teldrive_channel_id": 67890,
            "teldrive_target_path": "/",
            "teldrive_chunk_size": "1M",
            "teldrive_upload_concurrency": 1,
            "teldrive_random_chunk_name": False,
            "upload_max_retries": 1,
            "upload_min_throughput_kbps": 16,
            "upload_parallel_chunk_upload": False,
            "db_enabled": False,
        }
        base.update(overrides)
        return SimpleNamespace(**base)


class Tel2TelDriveRelayMessageTests(unittest.IsolatedAsyncioTestCase):
    def make_config(self, relay_enabled=True):
        return SimpleNamespace(
            telegram_channel_id=12345,
            relay_enabled=relay_enabled,
            db_enabled=False,
            teldrive_channel_id=67890,
        )

    async def run_handle_new_message(self, *, file_name, relay_enabled=True, mapping=None, td_files=None):
        manager = service_module.Tel2TelDriveService()
        relay_calls = []
        add_calls = []
        deleted_messages = []

        class FakeRelayManager:
            async def enqueue_message(self, client, config, msg, file_info):
                relay_calls.append((msg.id, dict(file_info)))

        class FakeClient:
            async def delete_messages(self, channel_id, message_ids):
                deleted_messages.append((channel_id, list(message_ids)))

        async def fake_run_blocking_io(func, *args, **kwargs):
            if func is service_module.load_mapping:
                return dict(mapping or {})
            if func is service_module.get_teldrive_files:
                return dict(td_files or {})
            if func is service_module.save_mapping:
                return None
            return func(*args, **kwargs)

        async def fake_add_file_to_teldrive(config, **kwargs):
            add_calls.append(dict(kwargs))
            return True

        original_extract = service_module.extract_file_info
        original_run_blocking_io = service_module.run_blocking_io
        original_add = service_module.add_file_to_teldrive
        try:
            manager.relay_manager = cast(Any, FakeRelayManager())
            service_module.extract_file_info = cast(
                Any,
                lambda msg: {"name": file_name, "size": 1024, "mime_type": "video/mp4"},
            )
            service_module.run_blocking_io = cast(Any, fake_run_blocking_io)
            service_module.add_file_to_teldrive = cast(Any, fake_add_file_to_teldrive)
            await manager.handle_new_message(
                cast(Any, FakeClient()),
                self.make_config(relay_enabled=relay_enabled),
                SimpleNamespace(id=77),
            )
        finally:
            service_module.extract_file_info = original_extract
            service_module.run_blocking_io = original_run_blocking_io
            service_module.add_file_to_teldrive = original_add

        return relay_calls, add_calls, deleted_messages

    async def test_relay_enabled_real_user_file_enqueues_relay_job(self):
        relay_calls, add_calls, deleted_messages = await self.run_handle_new_message(
            file_name="movie.mkv",
            relay_enabled=True,
        )

        self.assertEqual(len(relay_calls), 1)
        self.assertEqual(relay_calls[0][0], 77)
        self.assertEqual(relay_calls[0][1]["name"], "movie.mkv")
        self.assertEqual(add_calls, [])
        self.assertEqual(deleted_messages, [])

    async def test_relay_disabled_keeps_direct_teldrive_registration(self):
        relay_calls, add_calls, _ = await self.run_handle_new_message(
            file_name="movie.mkv",
            relay_enabled=False,
        )

        self.assertEqual(relay_calls, [])
        self.assertEqual(len(add_calls), 1)
        self.assertEqual(add_calls[0]["message_id"], 77)

    async def test_chunk_md5_and_duplicate_messages_do_not_enter_relay_queue(self):
        relay_calls, add_calls, _ = await self.run_handle_new_message(
            file_name="movie.mkv.1",
            relay_enabled=True,
        )
        self.assertEqual(relay_calls, [])
        self.assertEqual(add_calls, [])

        relay_calls, add_calls, _ = await self.run_handle_new_message(
            file_name="0" * 32,
            relay_enabled=True,
        )
        self.assertEqual(relay_calls, [])
        self.assertEqual(add_calls, [])

        relay_calls, add_calls, deleted_messages = await self.run_handle_new_message(
            file_name="movie.mkv",
            relay_enabled=True,
            mapping={"file-1": [10]},
            td_files={"file-1": {"name": "movie.mkv", "size": 1024}},
        )
        self.assertEqual(relay_calls, [])
        self.assertEqual(add_calls, [])
        self.assertEqual(deleted_messages, [(12345, [77])])
        self.assertEqual(service_module.filter_external_deleted_message_ids([77]), [])


class TelegramRelayManagerTests(TelegramRelayDatabaseMixin, unittest.IsolatedAsyncioTestCase):
    async def create_job(self, *, payload=b"telegram-file", status="pending"):
        config = self.make_config()
        job_id = "tgrelay-12345-77"
        local_path = str(Path(config.relay_download_dir) / job_id / "movie.mkv")
        job = await db.add_telegram_relay_job(
            job_id,
            source_channel_id=config.telegram_channel_id,
            source_message_id=77,
            file_name="movie.mkv",
            file_size=len(payload),
            mime_type="video/mp4",
            local_path=local_path,
        )
        if status != "pending":
            await db.update_telegram_relay_job(job_id, status=status, retry_count=3)
            job = await db.get_telegram_relay_job(job_id)
        return config, cast(dict, job)

    async def test_successful_relay_upload_marks_internal_delete_and_cleans_local_file(self):
        payload = b"hello relay"
        config, job = await self.create_job(payload=payload)
        client = FakeTelegramClient(payload)
        manager = TelegramRelayManager(FakeLogger(), FakeBroker())
        manager.client = client
        manager.config = config
        manager._stopped = False
        manager._semaphore = asyncio.Semaphore(1)

        async def fake_upload(path, config_, job_):
            return {
                "success": True,
                "data": {"id": "td-file-1"},
                "remote_parts": [{"partId": 9901}],
                "upload_meta": {"upload_id": "upload-1"},
            }

        async def fake_record(config_, job_, result):
            return "td-file-1"

        manager._upload_local_file = cast(Any, fake_upload)
        manager._record_teldrive_mapping = cast(Any, fake_record)

        await manager._process_job(job)

        saved = await db.get_telegram_relay_job(job["job_id"])
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(saved["teldrive_file_id"], "td-file-1")
        self.assertEqual(saved["upload_id"], "upload-1")
        self.assertEqual(client.deleted_messages, [(config.telegram_channel_id, [77])])
        self.assertFalse(Path(job["local_path"]).exists())
        self.assertEqual(service_module.filter_external_deleted_message_ids([77]), [])

    async def test_upload_failure_keeps_source_message_and_local_file_for_retry(self):
        payload = b"failed upload"
        config, job = await self.create_job(payload=payload)
        client = FakeTelegramClient(payload)
        manager = TelegramRelayManager(FakeLogger(), FakeBroker())
        manager.client = client
        manager.config = config
        manager._stopped = False
        manager._semaphore = asyncio.Semaphore(1)

        async def fake_upload(path, config_, job_):
            return {"success": False, "error": "upload exploded"}

        manager._upload_local_file = cast(Any, fake_upload)

        await manager._run_job(job["job_id"])

        saved = await db.get_telegram_relay_job(job["job_id"])
        self.assertEqual(saved["status"], "failed")
        self.assertIn("upload exploded", saved["error"])
        self.assertEqual(saved["retry_count"], 1)
        self.assertEqual(client.deleted_messages, [])
        self.assertTrue(Path(job["local_path"]).exists())

    async def test_retry_resets_failed_job_retry_count(self):
        config, job = await self.create_job(status="failed")
        manager = TelegramRelayManager(FakeLogger(), FakeBroker())
        manager.config = config
        manager._stopped = True

        result = await manager.retry_job(job["job_id"])

        self.assertTrue(result["success"])
        saved = await db.get_telegram_relay_job(job["job_id"])
        self.assertEqual(saved["status"], "pending")
        self.assertEqual(saved["retry_count"], 0)

    async def test_completed_job_cannot_be_retried(self):
        config, job = await self.create_job(status="completed")
        manager = TelegramRelayManager(FakeLogger(), FakeBroker())
        manager.config = config
        manager._stopped = True

        result = await manager.retry_job(job["job_id"])

        self.assertFalse(result["success"])
        saved = await db.get_telegram_relay_job(job["job_id"])
        self.assertEqual(saved["status"], "completed")


if __name__ == "__main__":
    unittest.main()
