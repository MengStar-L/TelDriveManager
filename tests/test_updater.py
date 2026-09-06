import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app import updater
import update_worker


class _Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Client:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, *args, **kwargs):
        return self.response


class UpdaterTests(unittest.IsolatedAsyncioTestCase):
    def test_version_order(self):
        self.assertGreater(updater._version("v2.0.0"), updater._version("1.9.9"))
        self.assertLess(updater._version("1.0.0-beta"), updater._version("1.0.0"))

    async def test_github_404_is_no_release(self):
        manager = updater.UpdateManager()
        response = _Response(status_code=404)
        with patch.object(updater.httpx, "AsyncClient", return_value=_Client(response)):
            state = await manager.check(force=True)
        self.assertEqual(state["state"], "no_release")
        self.assertFalse(state["update_available"])

    async def test_apply_check_does_not_deadlock(self):
        manager = updater.UpdateManager()
        manager.shutdown_callback = lambda: None
        manager.check = AsyncMock(return_value={"update_available": False})
        result = await asyncio.wait_for(manager.apply(), timeout=1)
        self.assertFalse(result["success"])

    def test_safe_extract_rejects_traversal_and_symlink(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            archive = root / "bad.zip"
            with zipfile.ZipFile(archive, "w") as handle:
                handle.writestr("../outside.txt", "bad")
            with self.assertRaises(RuntimeError):
                updater._safe_extract_zip(archive, root / "extract")

            archive = root / "link.zip"
            info = zipfile.ZipInfo("link")
            info.external_attr = 0o120000 << 16
            with zipfile.ZipFile(archive, "w") as handle:
                handle.writestr(info, "target")
            with self.assertRaises(RuntimeError):
                updater._safe_extract_zip(archive, root / "extract-link")

    def test_protected_paths(self):
        self.assertTrue(updater._protected("config.toml"))
        self.assertTrue(updater._protected("downloads/file.bin"))
        self.assertTrue(updater._protected("logs/app.log"))
        self.assertFalse(updater._protected("app/main.py"))

    def test_marker_is_removed_when_originally_absent(self):
        with tempfile.TemporaryDirectory() as folder:
            project = Path(folder) / "project"
            stage = Path(folder) / "stage"
            backup = Path(folder) / "backup"
            source = stage / "source"
            (project / "app").mkdir(parents=True)
            source_app = source / "app"
            source_app.mkdir(parents=True)
            (project / "main.py").write_text("old", encoding="utf-8")
            (project / "app" / "main.py").write_text("old-app", encoding="utf-8")
            (project / "config.toml").write_text("[aria2]\ndisk_protection_threshold_gb=1\n", encoding="utf-8")
            (source / "main.py").write_text("new", encoding="utf-8")
            (source_app / "main.py").write_text("new-app", encoding="utf-8")
            manifest = {
                "source_root": str(source),
                "files": ["main.py", "app/main.py"],
                "version": "9.9.9",
            }
            with patch.object(
                update_worker.shutil,
                "disk_usage",
                return_value=SimpleNamespace(free=10**15),
            ):
                update_worker.backup_and_install(project, stage, backup, manifest)
            self.assertEqual((project / ".tdm-version").read_text(encoding="utf-8").strip(), "9.9.9")
            saved = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
            update_worker.rollback(project, backup, saved["files"], marker_existed=saved["marker_existed"])
            self.assertFalse((project / ".tdm-version").exists())
            self.assertEqual((project / "main.py").read_text(encoding="utf-8"), "old")

    def test_restart_cwd_is_main_file_parent(self):
        with tempfile.TemporaryDirectory() as folder:
            main_file = Path(folder) / "main.py"
            main_file.write_text("", encoding="utf-8")
            with patch.object(update_worker.subprocess, "Popen", return_value=object()) as popen:
                update_worker.start_process([sys.executable, str(main_file)])
            kwargs = popen.call_args.kwargs
            self.assertEqual(Path(kwargs["cwd"]), main_file.parent)


if __name__ == "__main__":
    unittest.main()
