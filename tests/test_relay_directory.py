import tempfile
import unittest
from dataclasses import replace
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException

from app import config
from app.routes import settings
from app.modules.tel2teldrive import service, routes, relay


class RelayDirectoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_cloud_target_saves_and_hot_reloads_without_changing_local_or_default_directory(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.toml"
            path.write_text('[teldrive]\ntarget_path="/default"\n[telegram_relay]\ndownload_dir="./keep"\nconcurrency=3\n')
            store = service.ConfigStore(path)
            with patch.object(config, "CONFIG_PATH", path), patch.object(config, "_config_cache", None), \
                    patch.object(service, "config_store", store), \
                    patch.object(settings.aria2_service, "handle_config_update", AsyncMock()), \
                    patch.object(settings.task_manager, "reload_config", AsyncMock()), \
                    patch.object(settings.pikpak_routes, "reset_clients", AsyncMock()), \
                    patch.object(settings.db, "prune_progress_logs", AsyncMock()), \
                    patch.object(service.service.relay_manager, "apply_config", AsyncMock()) as apply, \
                    patch.object(service.service, "request_reload", AsyncMock()) as reload, \
                    patch.object(settings, "prepare_relay_download_dir") as local_directory:
                config.save_config({})
                store.reload()
                result = await settings.update_settings({"telegram_relay": {"target_path": " /relay//video/ "}})
                self.assertTrue(result["success"])
                saved = config.reload_config()
                self.assertEqual(saved["telegram_relay"]["target_path"], "/relay/video")
                self.assertEqual(saved["telegram_relay"]["download_dir"], "./keep")
                self.assertEqual(saved["telegram_relay"]["concurrency"], 3)
                self.assertEqual(saved["teldrive"]["target_path"], "/default")
                self.assertEqual(store.runtime().relay_target_path, "/relay/video")
                self.assertEqual(store.payload()["telegram_relay"]["target_path"], "/relay/video")
                apply.assert_awaited_once()
                reload.assert_not_awaited()
                local_directory.assert_not_called()

    async def test_invalid_cloud_target_is_rejected_before_any_configuration_write(self):
        for value in ("/a/../b", "https://example.invalid/path", "C:\\files", "/bad\x00path", ["/folder"]):
            with self.subTest(value=value), patch.object(settings, "load_config", return_value={}), \
                    patch.object(settings, "save_config") as save:
                with self.assertRaises(HTTPException) as error:
                    await settings.update_settings({"telegram_relay": {"target_path": value}})
                self.assertEqual(error.exception.status_code, 422)
                save.assert_not_called()

    async def test_cloud_folders_include_later_pages_and_exclude_files(self):
        runtime = SimpleNamespace(teldrive_url="http://unused.invalid", bearer_token="test")
        responses = [Mock(), Mock()]
        responses[0].json.return_value = {"items": [{"id": "a", "name": "Alpha", "type": "folder"},
                                                   {"id": "f", "name": "file.bin", "type": "file"}],
                                            "meta": {"totalPages": 2}}
        responses[1].json.return_value = {"items": [{"id": "z", "name": "Zulu", "type": "folder"}],
                                            "meta": {"totalPages": 2}}
        with patch.object(routes, "_get_deps", return_value=(None, None, Mock(runtime=lambda: runtime), None)), \
                patch.object(service.requests, "get", side_effect=responses) as request:
            result = await routes.get_teldrive_folders("/parent/")
        self.assertEqual(result, {"path": "/parent", "parent_path": "/", "folders": [
            {"name": "Alpha", "path": "/parent/Alpha"}, {"name": "Zulu", "path": "/parent/Zulu"}]})
        self.assertEqual(request.call_args_list[1].kwargs["params"]["page"], 2)

    async def test_folder_read_failure_is_visible_instead_of_showing_empty_success(self):
        runtime = SimpleNamespace(teldrive_url="http://unused.invalid", bearer_token="test")
        with patch.object(routes, "_get_deps", return_value=(None, None, Mock(runtime=lambda: runtime), None)), \
                patch.object(service, "list_teldrive_dir", side_effect=RuntimeError("page 2 unavailable")):
            with self.assertRaises(HTTPException) as error:
                await routes.get_teldrive_folders("/")
        self.assertEqual(error.exception.status_code, 502)
        self.assertIn("page 2 unavailable", error.exception.detail)

    async def test_first_upload_uses_latest_cloud_setting_or_legacy_default(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "file.bin"
            path.write_bytes(b"verified")
            runtime = service.ConfigStore(path.parent / "config.toml").runtime()
            for destination, expected in (("/relay/new", "/relay/new"), ("", "/legacy/default"), ("/", "/")):
                with self.subTest(destination=destination):
                    manager = relay.TelegramRelayManager(Mock(), Mock())
                    old = replace(runtime, relay_target_path="/relay/old")
                    manager.config = replace(runtime, relay_target_path=destination, teldrive_target_path="/legacy/default")
                    client = Mock(chunk_size=1024, upload_file_chunked=AsyncMock(return_value={"success": True}))
                    job = {"job_id": "first-upload"}
                    with patch.object(relay, "TelDriveClient", return_value=client), \
                            patch.object(relay.db, "update_telegram_relay_job", AsyncMock()) as update:
                        await manager._upload_local_file(path, old, job)
                    self.assertEqual(client.upload_file_chunked.call_args.args[1], expected)
                    self.assertEqual(update.call_args_list[0].kwargs, {"target_path": expected})

    async def test_unwritable_directory_returns_422_without_saving_config(self):
        previous = {"telegram_relay": {"download_dir": "./telegram_relay"}}
        with patch.object(settings, "load_config", return_value=previous), \
                patch.object(settings, "save_config") as save, \
                patch.object(config.tempfile, "TemporaryFile", side_effect=PermissionError(13, "Permission denied")):
            with tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(HTTPException) as error:
                    await settings.update_settings({"telegram_relay": {"download_dir": folder}})
            self.assertEqual(error.exception.status_code, 422)
            self.assertIn("Permission denied", error.exception.detail)
            save.assert_not_called()

    async def test_local_directory_creation_relative_absolute_home_and_file_rejection(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            with patch.object(config, "CONFIG_PATH", root / "config.toml"):
                path = config.prepare_relay_download_dir("./storage/relay files")
                self.assertEqual(path, root / "storage" / "relay files")
                self.assertEqual(list(path.iterdir()), [])
                self.assertEqual(config.prepare_relay_download_dir(str(path)), path)
                self.assertEqual(config.resolve_relay_download_dir(""), root / "telegram_relay")
                self.assertEqual(config.resolve_relay_download_dir("~/relay"), (Path.home() / "relay").resolve())
                file = root / "existing.txt"
                file.write_text("keep")
                with self.assertRaises(ValueError):
                    config.prepare_relay_download_dir(str(file))
                self.assertEqual(file.read_text(), "keep")
                with self.assertRaises(ValueError):
                    config.prepare_relay_download_dir("https://example.invalid/folder")

    async def test_directory_setting_survives_config_save_reload_and_keeps_other_fields(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "config.toml"
            path.write_text('[telegram_relay]\ndownload_dir="./before"\nconcurrency=3\n')
            with patch.object(config, "CONFIG_PATH", path), patch.object(config, "_config_cache", None):
                config.save_config({"telegram_relay": {"download_dir": "./after"}})
                reloaded = config.reload_config()["telegram_relay"]
                self.assertEqual(reloaded["download_dir"], "./after")
                self.assertEqual(reloaded["concurrency"], 3)
