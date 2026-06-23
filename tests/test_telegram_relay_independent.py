import asyncio
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from app import database as db
from app.modules.tel2teldrive import relay as relay_module
from app.modules.tel2teldrive import service as service_module


class FakeBroker:
    def __init__(self):
        self.events = []

    async def _broadcast(self, event):
        self.events.append(event)


class FakeLogger:
    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message):
        pass


class FakeRelayClient:
    def __init__(self, payload: bytes = b"relay-data"):
        self.payload = payload
        self.downloads = []
        self.deleted = []

    async def get_messages(self, channel_id, ids):
        return SimpleNamespace(id=ids, channel_id=channel_id)

    async def download_media(self, message, file, progress_callback=None):
        path = Path(file)
        path.write_bytes(self.payload)
        if progress_callback:
            progress_callback(len(self.payload), len(self.payload))
        self.downloads.append((message, str(path)))
        return str(path)

    async def delete_messages(self, channel_id, ids):
        self.deleted.append((channel_id, list(ids)))


class FakeRelayManager:
    def __init__(self):
        self.enqueued = []

    async def enqueue_message(self, client, config, msg, file_info):
        self.enqueued.append((client, config, msg, dict(file_info)))
        return {"job_id": "fake-job"}


def make_runtime(**overrides):
    base = dict(
        relay_enabled=True,
        relay_session_name="relay-test-session",
        relay_concurrency=1,
        relay_max_retries=1,
        relay_download_dir=tempfile.mkdtemp(prefix="relay-test-"),
        telegram_channel_id=12345,
        telegram_api_id=1,
        telegram_api_hash="hash",
        relay_proxy_type="socks5",
        relay_proxy_host="",
        relay_proxy_port=1080,
        relay_proxy_username="",
        relay_proxy_password="",
        teldrive_url="http://teldrive",
        bearer_token="token",
        teldrive_channel_id=54321,
        teldrive_chunk_size="100M",
        teldrive_upload_concurrency=1,
        teldrive_random_chunk_name=False,
        upload_max_retries=1,
        upload_min_throughput_kbps=100,
        upload_parallel_chunk_upload=False,
        teldrive_target_path="/",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class TelegramRelayIndependentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await db.init_db()

    async def asyncTearDown(self):
        await db.close_db()

    async def test_settings_put_relay_only_change_does_not_reload_main_listener(self):
        from app.routes import settings as settings_routes

        old_runtime = service_module.config_store.runtime()
        new_runtime = replace(old_runtime, relay_enabled=not old_runtime.relay_enabled)
        reload_calls = []
        apply_calls = []

        async def fake_handle_config_update(previous, current):
            return None

        async def fake_reload_config():
            return None

        async def fake_reset_clients():
            return None

        async def fake_prune(*args, **kwargs):
            return None

        async def fake_request_reload():
            reload_calls.append(True)

        async def fake_apply_config(runtime):
            apply_calls.append(runtime)

        original_load_config = settings_routes.load_config
        original_save_config = settings_routes.save_config
        original_reload_config = settings_routes.reload_config
        original_aria2_update = settings_routes.aria2_service.handle_config_update
        original_task_reload = settings_routes.task_manager.reload_config
        original_reset_clients = settings_routes.pikpak_routes.reset_clients
        original_prune = settings_routes.db.prune_progress_logs
        original_runtime = service_module.config_store.runtime
        original_reload = service_module.config_store.reload
        original_request_reload = service_module.service.request_reload
        original_apply_config = service_module.service.relay_manager.apply_config
        try:
            settings_routes.load_config = cast(Any, lambda force_reload=False: {"log": {"buffer_size": 400}})
            settings_routes.save_config = cast(Any, lambda payload: None)
            settings_routes.reload_config = cast(Any, lambda: {"log": {"buffer_size": 400}})
            settings_routes.aria2_service.handle_config_update = fake_handle_config_update
            settings_routes.task_manager.reload_config = fake_reload_config
            settings_routes.pikpak_routes.reset_clients = fake_reset_clients
            settings_routes.db.prune_progress_logs = fake_prune
            service_module.config_store.runtime = cast(Any, lambda: old_runtime)
            service_module.config_store.reload = cast(Any, lambda: new_runtime)
            service_module.service.request_reload = fake_request_reload
            service_module.service.relay_manager.apply_config = fake_apply_config

            result = await settings_routes.update_settings({"telegram_relay": {"enabled": new_runtime.relay_enabled}})
        finally:
            settings_routes.load_config = original_load_config
            settings_routes.save_config = original_save_config
            settings_routes.reload_config = original_reload_config
            settings_routes.aria2_service.handle_config_update = original_aria2_update
            settings_routes.task_manager.reload_config = original_task_reload
            settings_routes.pikpak_routes.reset_clients = original_reset_clients
            settings_routes.db.prune_progress_logs = original_prune
            service_module.config_store.runtime = original_runtime
            service_module.config_store.reload = original_reload
            service_module.service.request_reload = original_request_reload
            service_module.service.relay_manager.apply_config = original_apply_config

        self.assertTrue(result["success"])
        self.assertEqual(reload_calls, [])
        self.assertEqual(apply_calls, [new_runtime])

    async def test_relay_enqueue_is_idempotent_for_same_source_message(self):
        manager = relay_module.TelegramRelayManager(FakeLogger(), FakeBroker())
        config = make_runtime()
        schedule_calls = []
        msg = SimpleNamespace(id=9876)
        file_info = {"name": "movie.mkv", "size": 123, "mime_type": "video/x-matroska"}

        async def fake_apply_config(runtime):
            manager.config = runtime
            manager._stopped = False
            manager._semaphore = asyncio.Semaphore(1)

        async def fake_schedule(job_id):
            if job_id in manager._tasks:
                return
            schedule_calls.append(job_id)
            manager._tasks[job_id] = asyncio.create_task(asyncio.sleep(60))

        original_apply_config = manager.apply_config
        original_schedule = manager._schedule
        try:
            manager.apply_config = cast(Any, fake_apply_config)
            manager._schedule = cast(Any, fake_schedule)
            first = await manager.enqueue_message(object(), config, msg, file_info)
            second = await manager.enqueue_message(object(), config, msg, file_info)
        finally:
            for task in manager._tasks.values():
                task.cancel()
            await asyncio.gather(*manager._tasks.values(), return_exceptions=True)
            manager._tasks.clear()
            manager.apply_config = original_apply_config
            manager._schedule = original_schedule
            await db.delete_telegram_relay_job(first["job_id"])

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(first["source_message_id"], second["source_message_id"])
        self.assertEqual(schedule_calls, [first["job_id"]])

    async def test_relay_uses_injected_main_client(self):
        manager = relay_module.TelegramRelayManager(FakeLogger(), FakeBroker())
        sentinel = object()
        # 回源不再构造独立客户端；它通过注入的 getter 复用主监听客户端。
        self.assertIsNone(manager._current_client())
        manager.bind_client_getter(lambda: sentinel)
        self.assertIs(manager._current_client(), sentinel)

    async def test_refresh_auth_state_tracks_main_client(self):
        manager = relay_module.TelegramRelayManager(FakeLogger(), FakeBroker())

        class FakeAuthClient:
            def __init__(self, connected, authorized):
                self._connected = connected
                self._authorized = authorized

            def is_connected(self):
                return self._connected

            async def is_user_authorized(self):
                return self._authorized

        manager.bind_client_getter(lambda: FakeAuthClient(True, True))
        await manager._refresh_auth_state()
        self.assertTrue(manager.state_snapshot()["authorized"])
        self.assertEqual(manager.state_snapshot()["phase"], "authorized")

        manager.bind_client_getter(lambda: FakeAuthClient(True, False))
        await manager._refresh_auth_state()
        self.assertFalse(manager.state_snapshot()["authorized"])
        self.assertEqual(manager.state_snapshot()["phase"], "waiting_main")

        manager.bind_client_getter(lambda: None)
        self.assertFalse(await manager._is_authorized())

    async def test_main_listener_enqueues_forwarded_video_when_relay_enabled(self):
        service = service_module.Tel2TelDriveService()
        fake_relay = FakeRelayManager()
        service.relay_manager = cast(Any, fake_relay)
        config = make_runtime()
        client = object()
        video_attr = service_module.DocumentAttributeVideo(duration=10, w=1920, h=1080)
        media = service_module.MessageMediaDocument(
            document=SimpleNamespace(
                mime_type="video/mp4",
                size=123456,
                attributes=[video_attr],
            )
        )
        msg = SimpleNamespace(id=3456, media=media)

        async def fake_run_blocking_io(func, *args, **kwargs):
            if func is service_module.load_mapping:
                return {}
            if func is service_module.get_teldrive_files:
                return {}
            raise AssertionError(f"unexpected blocking call: {func}")

        original_run_blocking_io = service_module.run_blocking_io
        try:
            service_module.run_blocking_io = cast(Any, fake_run_blocking_io)
            await service.handle_new_message(cast(Any, client), config, msg)
        finally:
            service_module.run_blocking_io = original_run_blocking_io

        self.assertEqual(len(fake_relay.enqueued), 1)
        enqueued_client, enqueued_config, enqueued_msg, file_info = fake_relay.enqueued[0]
        self.assertIs(enqueued_client, client)
        self.assertIs(enqueued_config, config)
        self.assertIs(enqueued_msg, msg)
        self.assertEqual(file_info["name"], "video_3456.mp4")
        self.assertEqual(file_info["mime_type"], "video/mp4")

    async def test_relay_job_uses_main_client_for_download_and_delete(self):
        manager = relay_module.TelegramRelayManager(FakeLogger(), FakeBroker())
        fake_client = FakeRelayClient()
        config = make_runtime()
        manager.config = config
        manager._stopped = False
        manager._semaphore = asyncio.Semaphore(1)
        job_id = relay_module.make_relay_job_id(config.telegram_channel_id, 222)
        job = await db.add_telegram_relay_job(
            job_id,
            source_channel_id=config.telegram_channel_id,
            source_message_id=222,
            file_name="relay.bin",
            file_size=len(fake_client.payload),
            mime_type="application/octet-stream",
            local_path=str(Path(config.relay_download_dir) / job_id / "relay.bin"),
        )
        remembered_ids = []

        async def fake_upload(path, runtime, current_job):
            self.assertEqual(Path(path).read_bytes(), fake_client.payload)
            return {"success": True, "data": {"id": "td-file-1"}}

        manager.bind_client_getter(lambda: fake_client)
        original_upload = manager._upload_local_file
        original_remember = service_module.remember_internal_deleted_message_ids
        try:
            manager._upload_local_file = cast(Any, fake_upload)
            service_module.remember_internal_deleted_message_ids = cast(Any, lambda ids: remembered_ids.extend(ids))

            await manager._process_job(job)
            completed = await db.get_telegram_relay_job(job_id)
        finally:
            manager._upload_local_file = original_upload
            service_module.remember_internal_deleted_message_ids = original_remember
            await db.delete_telegram_relay_job(job_id)

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(fake_client.deleted, [(config.telegram_channel_id, [222])])
        self.assertEqual(remembered_ids, [222])

    async def test_download_speed_broadcast_only_while_downloading(self):
        broker = FakeBroker()
        manager = relay_module.TelegramRelayManager(FakeLogger(), broker)
        config = make_runtime()
        manager.config = config
        job_id = relay_module.make_relay_job_id(config.telegram_channel_id, 333)
        await db.add_telegram_relay_job(
            job_id,
            source_channel_id=config.telegram_channel_id,
            source_message_id=333,
            file_name="speed.bin",
            file_size=1000,
            mime_type="application/octet-stream",
            local_path=str(Path(config.relay_download_dir) / job_id / "speed.bin"),
        )
        try:
            await db.update_telegram_relay_job(job_id, status="downloading")
            await manager._update_download_progress(job_id, 50.0, 2048.0)
            dl_events = [e for e in broker.events if e["type"] == "relay_job_update"]
            self.assertTrue(dl_events)
            self.assertEqual(dl_events[-1]["payload"]["download_speed"], 2048.0)

            await db.update_telegram_relay_job(job_id, status="uploading")
            await manager._broadcast_job(await db.get_telegram_relay_job(job_id))
            up_events = [e for e in broker.events if e["type"] == "relay_job_update"]
            self.assertEqual(up_events[-1]["payload"]["download_speed"], 0.0)
            self.assertNotIn(job_id, manager._download_speed)
        finally:
            await db.delete_telegram_relay_job(job_id)

    def test_mapping_load_migrates_legacy_file_to_runtime_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / "legacy_file_msg_map.json"
            current = root / "history" / "tel2teldrive" / "file_msg_map.json"
            legacy.write_text('{"file-a": [101, "102"]}', encoding="utf-8")

            original_mapping_path = service_module.MAPPING_PATH
            original_legacy_path = service_module.LEGACY_MAPPING_PATH
            try:
                service_module.MAPPING_PATH = current
                service_module.LEGACY_MAPPING_PATH = legacy
                mapping = service_module.load_mapping()
            finally:
                service_module.MAPPING_PATH = original_mapping_path
                service_module.LEGACY_MAPPING_PATH = original_legacy_path

            self.assertEqual(mapping, {"file-a": [101, 102]})
            self.assertTrue(current.exists())


if __name__ == "__main__":
    unittest.main()
