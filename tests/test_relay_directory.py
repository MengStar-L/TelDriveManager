import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app import config
from app.routes import settings


class RelayDirectoryTests(unittest.IsolatedAsyncioTestCase):
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
