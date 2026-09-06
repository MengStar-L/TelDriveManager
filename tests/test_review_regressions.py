import asyncio
import json
import os
import tempfile
import unittest
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from app import auth, config, database as db
from app.disk_budget import DiskBudget, DiskSpaceUnavailable
from app.modules.aria2teldrive.task_manager import TaskManager
from app.modules.aria2teldrive.teldrive_client import TelDriveClient
from app.modules.pikpak.client import PikPakClient
from app.modules.pikpak import routes as pikpak_routes
from app.modules.tel2teldrive import relay, service


def runtime(**overrides):
    fields = dict(telegram_channel_id=12345, teldrive_channel_id=12345,
                  telegram_channel_conflict=False, db_enabled=False, sync_enabled=True,
                  teldrive_url="http://unused.invalid", bearer_token="test",
                  sync_interval=1, confirm_cycles=3, relay_enabled=True,
                  relay_concurrency=1, relay_max_retries=1)
    fields.update(overrides)
    return SimpleNamespace(**fields)


class SecurityRegressions(unittest.TestCase):
    def test_cached_token_cannot_bypass_expiry_or_password_change(self):
        settings = {"auth": {"username": "test", "password": "before"}}
        with patch.object(auth, "load_config", return_value=settings), patch.object(auth.time, "time", return_value=1000):
            token = auth.create_token()
            auth._active_tokens.add(token)
            self.addCleanup(auth._active_tokens.discard, token)
            self.assertTrue(auth.verify_token(token))
            with patch.object(auth.time, "time", return_value=1001 + auth.TOKEN_MAX_AGE):
                self.assertFalse(auth.verify_token(token))
            settings["auth"]["password"] = "after"
            self.assertFalse(auth.verify_token(token))

    def test_disk_full_config_write_preserves_original_and_cache(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.toml"
            original = b'[auth]\nusername="test"\npassword="before"\n'
            path.write_bytes(original)
            def fail_dump(data, handle):
                handle.write(b"broken")
                raise OSError(28, "No space left on device")
            with patch.object(config, "CONFIG_PATH", path), patch.object(config, "_config_cache", None):
                cached = config.load_config()
                with patch.object(config.tomli_w, "dump", side_effect=fail_dump):
                    with self.assertRaises(OSError):
                        config.save_config({"auth": {"password": "after"}})
                self.assertEqual(path.read_bytes(), original)
                self.assertIs(config.load_config(), cached)
                self.assertEqual(list(Path(folder).iterdir()), [path])
                config.save_config({"auth": {"password": "after"}})
                self.assertEqual(config.reload_config()["auth"]["password"], "after")

    def test_empty_existing_config_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.toml"
            path.touch()
            with patch.object(config, "CONFIG_PATH", path), patch.object(config, "_config_cache", None):
                with self.assertRaises(ValueError):
                    config.load_config()

    def test_mapping_replace_failure_preserves_original(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "mapping.json"
            path.write_text('{"old": [101]}', encoding="utf-8")
            with patch.object(service, "MAPPING_PATH", path), patch.object(service.os, "replace", side_effect=OSError("full")):
                with self.assertRaises(OSError):
                    service.save_mapping({"new": [202]})
            self.assertEqual(json.loads(path.read_text()), {"old": [101]})


class DeletionRegressions(unittest.IsolatedAsyncioTestCase):
    def test_failed_later_page_invalidates_entire_snapshot(self):
        response = Mock()
        response.json.return_value = {"items": [{"id": "live", "name": "live.bin"}], "meta": {"totalPages": 2}}
        with patch.object(service.requests, "get", side_effect=[response, OSError("503")]):
            with self.assertRaises(RuntimeError):
                service.get_teldrive_files(runtime())

    async def test_failed_snapshots_never_trigger_source_deletion(self):
        sleeps = 0
        async def poll(_):
            nonlocal sleeps
            sleeps += 1
            if sleeps > 3:
                raise asyncio.CancelledError()
        with patch.object(service, "get_teldrive_files", side_effect=[{"live": {"name": "live.bin"}}] + [OSError("503")] * 3), \
                patch.object(service.asyncio, "sleep", poll), \
                patch.object(service, "delete_telegram_messages_with_audit", AsyncMock()) as delete:
            with suppress(asyncio.CancelledError):
                await service.sync_deletions(SimpleNamespace(), runtime())
            delete.assert_not_awaited()

    async def test_confirmation_counts_once_and_failed_delete_keeps_mapping(self):
        mapping = {"live": [101]}
        cycle = 0
        deleted_at = []
        async def poll(_):
            nonlocal cycle
            cycle += 1
            if cycle > 4:
                raise asyncio.CancelledError()
        async def delete(*args, **kwargs):
            deleted_at.append(cycle)
            return len(deleted_at) > 1
        def save(value):
            mapping.clear()
            mapping.update(value)
        client = SimpleNamespace(get_messages=AsyncMock(return_value=[SimpleNamespace(id=101)]))
        with patch.object(service, "get_teldrive_files", side_effect=lambda _: {"live": {"name": "live.bin"}} if cycle == 0 else {}), \
                patch.object(service, "load_mapping", side_effect=lambda: dict(mapping)), \
                patch.object(service, "save_mapping", side_effect=save), \
                patch.object(service, "delete_telegram_messages_with_audit", delete), \
                patch.object(service.asyncio, "sleep", poll):
            with suppress(asyncio.CancelledError):
                await service.sync_deletions(client, runtime())
        self.assertEqual(deleted_at, [3, 4])
        self.assertEqual(mapping, {})

    async def test_authoritative_db_failure_blocks_stale_map_deletion(self):
        with patch.object(service, "load_mapping", return_value={"live": [101]}), \
                patch.object(service, "query_db_mapping", side_effect=OSError("unavailable")), \
                patch.object(service, "delete_file_from_teldrive", AsyncMock()) as delete:
            count = await service.delete_teldrive_files_for_missing_messages(
                runtime(db_enabled=True), [101], td_files={"live": {"name": "live.bin", "size": 4}})
            self.assertEqual(count, 0)
            delete.assert_not_awaited()

    async def test_disabled_sync_blocks_event_and_direct_call(self):
        handlers = []
        client = SimpleNamespace(on=lambda event: lambda callback: handlers.append(callback))
        manager = service.Tel2TelDriveService()
        cfg = runtime(sync_enabled=False)
        manager.register_handlers(client, cfg)
        with patch.object(service.config_store, "runtime", return_value=cfg), \
                patch.object(service, "delete_teldrive_files_for_missing_messages", AsyncMock()) as delete:
            await handlers[1](SimpleNamespace(deleted_ids=[101]))
            delete.assert_not_awaited()
        with patch.object(service, "load_mapping") as read:
            self.assertEqual(await service.delete_teldrive_files_for_missing_messages(cfg, [101]), 0)
            read.assert_not_called()

    async def test_same_filename_does_not_delete_or_link_new_source(self):
        manager = service.Tel2TelDriveService()
        manager.relay_manager.enqueue_message = AsyncMock()
        with patch.object(service, "extract_file_info", return_value={"name": "same.bin", "size": 8192}), \
                patch.object(service, "load_mapping", return_value={"old": [101]}), \
                patch.object(service, "get_teldrive_files", return_value={"old": {"name": "same.bin", "size": 1024}}), \
                patch.object(service, "save_mapping") as save, \
                patch.object(service, "delete_telegram_messages_with_audit", AsyncMock()) as delete:
            await manager.handle_new_message(SimpleNamespace(), runtime(), SimpleNamespace(id=202))
            manager.relay_manager.enqueue_message.assert_awaited_once()
            delete.assert_not_awaited()
            save.assert_not_called()


class TransferDatabaseRegressions(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await db.close_db()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.path_patch = patch.object(db, "DB_PATH", self.root / "tasks.db")
        self.path_patch.start()
        await db.init_db()

    async def asyncTearDown(self):
        await db.close_db()
        self.path_patch.stop()
        self.tmp.cleanup()

    def make_manager(self):
        manager = TaskManager()
        manager.config = {"aria2": {"download_dir": str(self.root)}, "teldrive": {"upload_dir": ""},
                          "upload": {"auto_delete": True}}
        manager._broadcast_task_update = AsyncMock()
        manager.broadcast = AsyncMock()
        manager._cleanup_orphan_parts = AsyncMock()
        return manager

    async def test_directory_resume_requires_each_file_commit_and_uploads_empty_files(self):
        folder = self.root / "directory"
        folder.mkdir()
        (folder / "a.bin").write_bytes(b"a" * 8)
        (folder / "b.bin").write_bytes(b"b" * 4)
        (folder / "empty.bin").touch()
        manager = self.make_manager()
        calls = []
        failed = False
        async def upload(path, target, progress, **kwargs):
            nonlocal failed
            name = Path(path).name
            calls.append(name)
            if name == "b.bin" and not failed:
                failed = True
                raise RuntimeError("injected before b upload")
            count = (Path(path).stat().st_size + 3) // 4
            numbers = list(range(1, count + 1))
            parts = [{"partNo": number, "partId": 100 + number} for number in numbers]
            if count:
                await kwargs["part_confirm_callback"](count, parts[-1], numbers, parts, count)
            return {"success": True, "data": {"id": name}, "upload_meta": {
                "upload_id": kwargs["upload_id"], "confirmed_part_numbers": numbers, "remote_parts": parts}}
        manager.teldrive = SimpleNamespace(chunk_size=4, upload_file_chunked=upload)
        await db.add_task("directory", "https://unused.invalid")
        await db.update_task("directory", status="uploading", local_path=str(folder), upload_session_token="owner",
                             upload_session_state="running", upload_confirmed_chunks=1, upload_confirmed_total=3)
        with self.assertRaises(RuntimeError):
            await manager._upload_directory("directory", str(folder), token="owner")
        task = await db.get_task("directory")
        self.assertEqual(task["upload_confirmed_chunks"], 2)
        self.assertIsNone(task["upload_finished_at"])
        self.assertTrue(folder.exists())
        await manager._upload_directory("directory", str(folder), token="owner")
        self.assertEqual(calls, ["a.bin", "b.bin", "b.bin", "empty.bin"])
        self.assertEqual((await db.get_task("directory"))["status"], "completed")

    async def test_clearing_failed_task_removes_owned_cache(self):
        path = self.root / "failed.bin"
        path.write_bytes(b"content")
        await db.add_task("failed", "https://unused.invalid")
        await db.update_task("failed", status="failed", local_path=str(path))
        self.assertTrue((await self.make_manager().delete_task("failed"))["success"])
        self.assertFalse(path.exists())
        self.assertIsNone(await db.get_task("failed"))

    async def test_all_chunks_checkpoint_does_not_skip_uncommitted_directory_file(self):
        folder = self.root / "dir"
        folder.mkdir()
        path = folder / "file.bin"
        path.write_bytes(b"data")
        manager = self.make_manager()
        upload = AsyncMock(return_value={"success": True, "data": {"id": "new"}})
        manager.teldrive = SimpleNamespace(chunk_size=4, upload_file_chunked=upload)
        await db.add_task("dir", "https://unused.invalid")
        await db.update_task("dir", status="uploading", upload_id="partial", upload_confirmed_chunks=1,
                             upload_confirmed_parts_json="[1]", upload_session_state="running", upload_session_token="owner",
                             upload_directory_state_json=json.dumps({"completed": {}, "current": "file.bin",
                                                                     "signature": [4, path.stat().st_mtime_ns]}))
        await manager._upload_directory("dir", str(folder), token="owner")
        upload.assert_awaited_once()
        self.assertEqual(upload.call_args.kwargs["confirmed_part_numbers"], [1])

    async def test_auto_cleanup_does_not_delete_another_tasks_cache(self):
        path = self.root / "shared.bin"
        path.write_bytes(b"data")
        for task_id, status in (("old", "completed"), ("new", "downloading")):
            await db.add_task(task_id, "https://unused.invalid")
            await db.update_task(task_id, status=status, local_path=str(path))
        self.assertFalse(await self.make_manager()._auto_delete_local("old", str(path)))
        self.assertTrue(path.exists())
        self.assertIn("Local cleanup pending", (await db.get_task("old"))["error"])

    async def test_disk_held_serial_task_recovers_without_two_gate_deadlock(self):
        from tests.test_serial_gate import FakeAria2
        manager = self.make_manager()
        manager.config["upload"]["serial_transfer_mode"] = True
        manager.config["aria2"]["disk_protection_threshold_gb"] = 1
        manager.aria2 = FakeAria2()
        manager._has_serial_resume_blockers = AsyncMock(return_value=False)
        manager._auto_retry_disk_failed_downloads = AsyncMock()
        await db.add_task("serial", "https://unused.invalid")
        await db.update_task("serial", status="pending", aria2_gid="serial")
        item = {"gid": "serial", "status": "paused", "dir": str(self.root), "totalLength": "4096", "completedLength": "0", "files": []}
        manager._hold_gid_for_disk_gate("serial")
        from app.disk_budget import disk_budget
        self.addCleanup(disk_budget.release, "aria2:serial")
        with patch("app.disk_budget.shutil.disk_usage", return_value=SimpleNamespace(free=0)):
            await manager._sync_disk_space_download_protection([], [item])
            await manager._normalize_serial_pending_aria2_tasks([], [item], [])
            await manager._sync_serial_transfer_gate([], [item], [])
        self.assertNotIn("serial", manager._serial_gate_paused_gids)
        self.assertEqual(manager.aria2.unpaused, [])
        manager._disk_usage_info = {"free": 2 * 1024 ** 3}
        with patch("app.disk_budget.shutil.disk_usage", return_value=SimpleNamespace(free=2 * 1024 ** 3)):
            await manager._sync_disk_space_download_protection([], [item])
        self.assertEqual(manager.aria2.unpaused, ["serial"])

    async def test_cache_deletion_failure_preserves_task_for_retry(self):
        path = self.root / "failed.bin"
        path.write_bytes(b"content")
        await db.add_task("failed", "https://unused.invalid")
        await db.update_task("failed", status="failed", local_path=str(path))
        with patch.object(os, "remove", side_effect=PermissionError("busy")):
            result = await self.make_manager().delete_task("failed")
        self.assertFalse(result["success"])
        self.assertIsNotNone(await db.get_task("failed"))
        self.assertTrue(path.exists())

    async def test_source_cleanup_waits_for_all_transfers_across_restart_and_record_deletion(self):
        for task_id in ("a", "b"):
            await db.add_task(task_id, "https://unused.invalid")
        await db.add_source_cleanup("account-a", ["owned-scope"], ["a", "b"])
        self.assertEqual(await db.get_ready_source_cleanups(), [])
        await db.update_task("a", status="completed")
        await db.delete_task("a")
        await db.close_db()
        await db.init_db()
        self.assertEqual(await db.get_ready_source_cleanups(), [])
        await db.update_task("b", status="completed")
        ready = await db.get_ready_source_cleanups()
        self.assertEqual(len(ready), 1)
        self.assertEqual(ready[0]["account_id"], "account-a")
        client = SimpleNamespace(delete_files=AsyncMock())
        with patch.object(pikpak_routes.pikpak_account_pool, "client_for_account", AsyncMock(return_value=(None, client))) as account:
            await self.make_manager()._cleanup_completed_sources()
            account.assert_awaited_once_with("account-a")
            client.delete_files.assert_awaited_once_with(["owned-scope"])
        self.assertEqual(await db.get_ready_source_cleanups(), [])

    async def make_relay(self):
        manager = relay.TelegramRelayManager(Mock(), SimpleNamespace(_broadcast=AsyncMock()))
        manager.config = runtime(relay_download_dir=str(self.root))
        manager._stopped = False
        manager._broadcast_job_id = AsyncMock()
        manager._is_authorized = AsyncMock(return_value=True)
        client = SimpleNamespace(get_messages=AsyncMock(return_value=SimpleNamespace()), download_media=AsyncMock())
        manager.bind_client_getter(lambda: client)
        job_id = "tgrelay-12345-101"
        path = manager._build_local_file_path(manager.config, job_id, "video.bin")
        await db.add_telegram_relay_job(job_id, source_channel_id=12345, source_message_id=101,
                                        file_name="video.bin", file_size=4096, local_path=str(path))
        async def download(_client, message, target, job):
            target.write_bytes(b"a" * 4096)
        manager._download_message = AsyncMock(side_effect=download)
        manager._upload_local_file = AsyncMock(return_value={"success": True, "data": {"id": "remote-file"}})
        manager._record_teldrive_mapping = AsyncMock(return_value="remote-file")
        return manager, job_id, path

    async def test_preallocated_file_is_redownloaded_without_verified_marker(self):
        manager, job_id, path = await self.make_relay()
        path.parent.mkdir()
        path.write_bytes(b"a" * 1024 + b"\0" * 3072)
        await db.update_telegram_relay_job(job_id, status="downloading", download_progress=25)
        with patch.object(service, "delete_telegram_messages_with_audit", AsyncMock(return_value=True)):
            await manager._process_job(await db.get_telegram_relay_job(job_id))
        manager._download_message.assert_awaited_once()
        self.assertEqual((await db.get_telegram_relay_job(job_id))["status"], "completed")

    async def test_cleanup_retry_does_not_reupload_or_redownload_after_restart(self):
        manager, job_id, path = await self.make_relay()
        with patch.object(service, "delete_telegram_messages_with_audit", AsyncMock(side_effect=[False, True])):
            with self.assertRaises(RuntimeError):
                await manager._process_job(await db.get_telegram_relay_job(job_id))
            self.assertFalse(path.exists())
            await db.close_db()
            await db.init_db()
            await manager._process_job(await db.get_telegram_relay_job(job_id))
        manager._upload_local_file.assert_awaited_once()
        manager._download_message.assert_awaited_once()

    async def test_legacy_failed_cleanup_migrates_to_committed_phase(self):
        manager, job_id, _ = await self.make_relay()
        await db.update_telegram_relay_job(job_id, status="failed", upload_progress=100,
                                          teldrive_file_id="legacy-file", error="source deletion failed")
        await db.init_db()
        job = await db.get_telegram_relay_job(job_id)
        self.assertEqual(job["upload_committed"], 1)
        with patch.object(service, "delete_telegram_messages_with_audit", AsyncMock(return_value=True)):
            await manager._process_job(job)
        manager._upload_local_file.assert_not_awaited()
        manager._download_message.assert_not_awaited()

    async def test_local_cleanup_error_is_not_reported_completed(self):
        manager, job_id, path = await self.make_relay()
        result = {"success": True, "data": {"id": "remote-file"}}
        await db.update_telegram_relay_job(job_id, upload_committed=1, upload_result_json=json.dumps(result))
        manager._cleanup_local_path = AsyncMock(side_effect=PermissionError("busy"))
        with self.assertRaises(PermissionError):
            await manager._process_job(await db.get_telegram_relay_job(job_id))
        self.assertEqual((await db.get_telegram_relay_job(job_id))["status"], "cleaning")
        manager._upload_local_file.assert_not_awaited()

    async def test_no_space_leaves_job_pending_and_releases_slot_without_downloading(self):
        manager, job_id, path = await self.make_relay()
        with patch("app.disk_budget.shutil.disk_usage", return_value=SimpleNamespace(free=0)):
            await manager._run_job(job_id)
        manager._download_message.assert_not_awaited()
        job = await db.get_telegram_relay_job(job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["retry_count"], 0)
        self.assertEqual(manager._semaphore.active, 0)

    async def test_relay_restart_removes_only_inactive_part_files(self):
        manager, job_id, path = await self.make_relay()
        path.parent.mkdir()
        path.write_bytes(b'complete cache retained')
        part = path.with_name(path.name + '.part')
        part.write_bytes(b'interrupted')
        active = asyncio.create_task(asyncio.Event().wait())
        manager._tasks[job_id] = active
        try:
            await manager._cleanup_inactive_parts()
            self.assertTrue(part.exists())
        finally:
            active.cancel()
            await asyncio.gather(active, return_exceptions=True)
            manager._tasks.clear()
        await manager._cleanup_inactive_parts()
        self.assertFalse(part.exists())
        self.assertEqual(path.read_bytes(), b'complete cache retained')

    async def test_relay_mid_download_disk_failure_removes_part_and_releases_slot(self):
        manager, job_id, path = await self.make_relay()
        async def interrupted(client, message, target, job):
            target.write_bytes(b'partial')
            raise DiskSpaceUnavailable('external disk consumption')
        manager._download_message = AsyncMock(side_effect=interrupted)
        await manager._run_job(job_id)
        self.assertFalse(path.with_name(path.name + '.part').exists())
        self.assertEqual(manager._semaphore.active, 0)
        self.assertEqual((await db.get_telegram_relay_job(job_id))['status'], 'pending')
        manager._upload_local_file.assert_not_awaited()

    async def test_retry_semaphore_waiter_reschedules_without_cancelling_caller(self):
        manager, job_id, _ = await self.make_relay()
        async with manager._semaphore:
            await manager._schedule(job_id)
            await asyncio.sleep(0)
            result = await manager.retry_job(job_id)
            self.assertTrue(result["success"])
            self.assertIn(job_id, manager._tasks)
            manager._stopped = True
            tasks = list(manager._tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


class UploadWorkerRegressions(unittest.IsolatedAsyncioTestCase):
    async def test_parent_cancel_waits_for_parallel_children_and_stops_checkpoints(self):
        client = TelDriveClient()
        client.chunk_size = 4
        client.upload_concurrency = 2
        started = asyncio.Event()
        active = 0
        async def upload(*args, **kwargs):
            nonlocal active
            active += 1
            if active == 2:
                started.set()
            try:
                await asyncio.Event().wait()
            finally:
                active -= 1
        client._upload_single_chunk = upload
        checkpoint = AsyncMock()
        task = asyncio.create_task(client._do_multi_upload(
            None, Path("unused"), "upload", "file", 8, 2, None, part_confirm_callback=checkpoint))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(active, 0)
        checkpoint.assert_not_awaited()

    async def test_bot_download_handles_short_writes_without_losing_bytes(self):
        async def parts(*args, **kwargs):
            yield b"x" * 4096
        client = SimpleNamespace(iter_download=parts)
        pool = relay.BotDownloadPool(1, "test", ["1:test"])
        pool._acquire = AsyncMock(return_value=[client])
        pool._resolve_doc = AsyncMock(return_value=object())
        def pwrite(fd, data, offset):
            os.lseek(fd, offset, os.SEEK_SET)
            return os.write(fd, data[:127])
        with tempfile.TemporaryDirectory() as folder, patch.object(os, "pwrite", pwrite, create=True):
            path = Path(folder) / "file.part"
            self.assertTrue(await pool.download(12345, 1, str(path), 4096, 1, None))
            self.assertEqual(path.read_bytes(), b"x" * 4096)

    async def test_bot_download_rejects_normal_early_eof(self):
        async def parts(*args, **kwargs):
            yield b"x" * 1024
        client = SimpleNamespace(iter_download=parts)
        pool = relay.BotDownloadPool(1, "test", ["1:test"])
        pool._acquire = AsyncMock(return_value=[client])
        pool._resolve_doc = AsyncMock(return_value=object())
        def pwrite(fd, data, offset):
            os.lseek(fd, offset, os.SEEK_SET)
            return os.write(fd, data)
        with tempfile.TemporaryDirectory() as folder, patch.object(os, "pwrite", pwrite, create=True), \
                patch.object(relay.asyncio, "sleep", AsyncMock()):
            success = await pool.download(12345, 1, str(Path(folder) / "file.part"), 4096, 1, None)
        self.assertFalse(success)

    async def test_dynamic_limiter_resize_keeps_one_shared_active_count(self):
        limiter = relay.DynamicLimiter(1)
        async with limiter:
            acquired = asyncio.Event()
            release = asyncio.Event()
            async def worker():
                async with limiter:
                    acquired.set()
                    await release.wait()
            waiter = asyncio.create_task(worker())
            await asyncio.sleep(0)
            self.assertFalse(acquired.is_set())
            limiter.resize(2)
            await asyncio.wait_for(acquired.wait(), 2)
            self.assertEqual(limiter.active, 2)
            limiter.resize(1)
            extra = asyncio.create_task(worker())
            await asyncio.sleep(0)
            self.assertEqual(limiter.active, 2)
            release.set()
            await waiter
        await extra
        self.assertEqual(limiter.active, 0)


class DiskBudgetRegressions(unittest.TestCase):
    def test_shared_volume_reservations_prevent_overcommit_and_release(self):
        budget = DiskBudget()
        with tempfile.TemporaryDirectory() as folder, patch("app.disk_budget.shutil.disk_usage", return_value=SimpleNamespace(free=100)):
            budget.reserve("relay:a", folder, 60, 10)
            with self.assertRaises(DiskSpaceUnavailable):
                budget.reserve("aria2:b", folder, 40, 10)
            budget.release("relay:a")
            budget.reserve("aria2:b", folder, 40, 10)

    def test_unrelated_volumes_do_not_consume_each_others_budget(self):
        budget = DiskBudget()
        with patch.object(budget, "filesystem", side_effect=lambda path: (path, path)), \
                patch("app.disk_budget.shutil.disk_usage", return_value=SimpleNamespace(free=100)):
            budget.reserve("relay:a", "volume1", 80, 10)
            budget.reserve("aria2:b", "volume2", 80, 10)


class PikPakRegressions(unittest.IsolatedAsyncioTestCase):
    async def test_selected_magnet_cleanup_only_targets_selected_files_on_their_account(self):
        client = SimpleNamespace(get_download_urls=AsyncMock(return_value=[
            {"file_id": "selected", "name": "a", "url": "https://unused/a"},
            {"file_id": "unselected", "name": "b", "url": "https://unused/b"}]))
        with patch.object(pikpak_routes, "load_config", return_value={"pikpak": {"delete_after_download": True}}), \
                patch.object(pikpak_routes.pikpak_account_pool, "client_for_account", AsyncMock(return_value=(SimpleNamespace(id="account"), client))), \
                patch.object(pikpak_routes, "_broadcast", AsyncMock()), \
                patch.object(pikpak_routes, "_broadcast_resolved_files", AsyncMock()), \
                patch.object(pikpak_routes, "_aria2_push_only", AsyncMock(return_value=["task"])) as push, \
                patch.object(db, "add_source_cleanup", AsyncMock()) as cleanup:
            await pikpak_routes._process_magnet_selected(["root"], ["selected"], root_accounts={"root": "account"})
        self.assertEqual(len(push.call_args.args[0]), 1)
        cleanup.assert_awaited_once_with("account", ["selected"], ["task"])

    async def test_failed_push_never_deletes_source(self):
        with patch.object(pikpak_routes, "_ensure_aria2_client", AsyncMock(return_value=SimpleNamespace(add_uris_batch=AsyncMock(side_effect=OSError("offline"))))), \
                patch.object(pikpak_routes, "load_config", return_value={"pikpak": {"delete_after_download": True}}), \
                patch.object(pikpak_routes, "_broadcast", AsyncMock()), \
                patch.object(pikpak_routes.pikpak_account_pool, "client_for_account", AsyncMock()) as account, \
                patch.object(db, "add_source_cleanup", AsyncMock()) as cleanup:
            await pikpak_routes._aria2_push_only([{"name": "file", "url": "https://unused.invalid"}], 1, ["source"], delete_account_id="account")
        account.assert_not_awaited()
        cleanup.assert_not_awaited()

    async def test_share_root_and_nested_folder_follow_every_page(self):
        file = lambda fid: {"id": fid, "kind": "drive#file", "name": fid, "size": 1}
        raw = SimpleNamespace(PIKPAK_API_HOST="unused.invalid")
        raw._request_get = AsyncMock(side_effect=[
            {"files": [file("a")], "next_page_token": "root2", "pass_code_token": "pass"},
            {"files": [{"id": "folder", "kind": "drive#folder", "name": "folder"}]},
            {"files": [file("c")]},
        ])
        raw.get_share_folder = AsyncMock(return_value={"files": [file("b")], "next_page_token": "folder2"})
        client = object.__new__(PikPakClient)
        client.client = raw
        result = await client.get_share_file_list("https://mypikpak.com/s/test")
        self.assertEqual([item["id"] for item in result["files"]], ["a", "b", "c"])
        self.assertEqual(raw._request_get.call_args_list[1].kwargs["params"]["page_token"], "root2")
        self.assertEqual(raw._request_get.call_args_list[2].kwargs["params"]["page_token"], "folder2")

    async def test_repeating_page_token_is_an_error(self):
        client = object.__new__(PikPakClient)
        client.client = SimpleNamespace(PIKPAK_API_HOST="unused.invalid",
                                       _request_get=AsyncMock(return_value={"files": [], "next_page_token": "repeat"}))
        with self.assertRaises(RuntimeError):
            await client._collect_share_pages({"files": [], "next_page_token": "repeat"}, "/drive/v1/share", {})
